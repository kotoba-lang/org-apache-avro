(ns avro.file
  "The Avro Object Container File: header, schema, blocks, sync markers.

      'O' 'b' 'j' 0x01
      metadata map           avro.schema (JSON), avro.codec
      16-byte sync marker
      block: count, byte-size, data, sync
      block: ...

  ## The sync marker is the integrity check, and it is checked

  Every block ends with a copy of the header's 16-byte marker. A reader that
  skips it will happily walk off the end of a truncated block and decode the
  next block's length as a record — producing values, which is the failure
  mode worth paying one comparison per block to avoid.

  ## Blocks carry their byte size, so a codec this reader refuses is skippable

  The block header is `(record-count, byte-count)`. That second number is what
  lets a file compressed with a codec we do not have still report **how many
  records it holds** — the Avro analogue of Parquet statistics being readable
  from a file whose pages are not. `records` refuses, `record-count` does not."
  (:require [avro.binary :as b]
            [avro.datum :as datum]
            [avro.schema :as schema]
            [deflate.core :as deflate]
            [zstd.core :as zstd]))

(def magic [0x4F 0x62 0x6A 0x01])          ; "Obj\1"
(def ^:private sync-size 16)

(def decodable-codecs
  "Codecs whose blocks this reader can undo.

  `snappy` is absent: Avro frames it with a 4-byte big-endian CRC-32C suffix
  that the Parquet reader's raw-block snappy does not carry, so wiring that
  decoder here would decode most blocks and corrupt the checksum on all of
  them. Refused by name instead."
  #{"null" "deflate" "zstandard"})

(defn- header
  "-> `{:schema .. :codec .. :sync [..] :at n}`."
  [bs]
  (when-not (= magic (vec (take 4 bs)))
    (throw (ex-info "not an Avro container file" {:type :avro/not-avro})))
  (let [[meta i] (loop [i 4 acc {}]
                   (let [[n i'] (b/long-at bs i)
                         n (long n)]
                     (if (zero? n)
                       [acc i']
                       (let [[cnt i'] (if (neg? n)
                                        (let [[_ i''] (b/long-at bs i')] [(- n) i''])
                                        [n i'])
                             [m i''] (reduce (fn [[a j] _]
                                               (let [[k j'] (b/string-at bs j)
                                                     [v j''] (b/bytes-at bs j')]
                                                 [(assoc a k v) j'']))
                                             [acc i'] (range cnt))]
                         (recur i'' m)))))
        codec (if-let [c (get meta "avro.codec")] (b/utf8 c) "null")]
    {:schema (schema/parse (b/utf8 (or (get meta "avro.schema")
                                       (throw (ex-info "file declares no avro.schema"
                                                       {:type :avro/malformed})))))
     :codec codec
     :sync (vec (subvec (vec bs) i (+ i sync-size)))
     :at (+ i sync-size)}))

(defn- decompress [codec block]
  (case codec
    "null" block
    "deflate" (vec (deflate/inflate-raw block))
    "zstandard" (vec (zstd/decompress block))
    (throw (ex-info (str "avro: codec " (pr-str codec) " is not implemented"
                         " — the record count is still readable from this file")
                    {:type :avro/unsupported-codec :codec codec
                     :decodable (vec decodable-codecs)}))))

(defn blocks
  "Every block's `{:count :size :at}` — **without decompressing anything**.

  This is the cheap call: it walks header-to-header using the declared byte
  sizes, verifying each sync marker, and never looks at a record. It is what
  makes `record-count` answerable for a codec this reader cannot undo."
  [bs]
  (let [{:keys [sync at]} (header bs)
        n (count bs)]
    (loop [i at out []]
      (if (>= i n)
        out
        (let [[cnt i'] (b/long-at bs i)
              [size i''] (b/long-at bs i')
              size (long size)
              data-end (+ i'' size)
              marker (vec (subvec (vec bs) data-end (+ data-end sync-size)))]
          (when-not (= sync marker)
            (throw (ex-info "block sync marker does not match the header's"
                            {:type :avro/desynchronised :at data-end})))
          (recur (+ data-end sync-size)
                 (conj out {:count (long cnt) :size size :at i''})))))))

(defn record-count
  "Total records, from block headers alone.

  Answerable on a file whose codec this reader refuses — the same property
  Parquet statistics have, and the reason a partial reader earns its keep."
  [bs]
  (reduce + 0 (map :count (blocks bs))))

(defn file-schema [bs] (:schema (header bs)))

(defn records
  "Every record in the file, decoded.

  Throws for a codec `decodable-codecs` does not name, **after** the block
  walk has succeeded — so a caller that wanted the count already has it."
  [bs]
  (let [{:keys [schema codec sync]} (header bs)
        _ sync]
    (into []
          (mapcat (fn [{:keys [count size at]}]
                    (let [raw (subvec (vec bs) at (+ at size))
                          block (decompress codec raw)]
                      (first (reduce (fn [[acc j] _]
                                       (let [[v j'] (datum/decode schema block j)]
                                         [(conj acc v) j']))
                                     [[] 0] (range count))))))
          (blocks bs))))
