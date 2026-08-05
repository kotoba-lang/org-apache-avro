(ns avro.datum
  "Decoding one value against a schema.

  Avro writes **no type tags**. The bytes of a long and the bytes of a string
  length are the same varint, and which one a position holds is known only
  from the schema. So decoding is schema-directed from the first byte, and a
  schema that disagrees with the writer's does not fail — it produces
  plausible wrong values and then desynchronises.

  That is why `avro.file` reads the schema out of the file rather than taking
  one from the caller: the only schema guaranteed to agree with the bytes is
  the one shipped beside them."
  (:require [avro.binary :as b]))

(declare decode)

(defn- decode-array [schema bs i]
  ;; Blocks of (count, items...) terminated by a zero count. A NEGATIVE count
  ;; means the block is followed by its byte size -- writers emit that so a
  ;; reader can skip a block without decoding it -- and reading the size as
  ;; another item is how that turns into garbage.
  (loop [i i out []]
    (let [[n i'] (b/long-at bs i)
          n (long n)]
      (cond
        (zero? n) [out i']
        :else
        (let [[cnt i'] (if (neg? n)
                         (let [[_size i''] (b/long-at bs i')] [(- n) i''])
                         [n i'])
              [items i''] (reduce (fn [[acc j] _]
                                    (let [[v j'] (decode (:items schema) bs j)]
                                      [(conj acc v) j']))
                                  [out i'] (range cnt))]
          (recur i'' items))))))

(defn- decode-map [schema bs i]
  (loop [i i out {}]
    (let [[n i'] (b/long-at bs i)
          n (long n)]
      (cond
        (zero? n) [out i']
        :else
        (let [[cnt i'] (if (neg? n)
                         (let [[_size i''] (b/long-at bs i')] [(- n) i''])
                         [n i'])
              [m i''] (reduce (fn [[acc j] _]
                                (let [[k j'] (b/string-at bs j)
                                      [v j''] (decode (:values schema) bs j')]
                                  [(assoc acc k v) j'']))
                              [out i'] (range cnt))]
          (recur i'' m))))))

(defn decode
  "One value of `schema` at `i` → `[value next-index]`."
  [schema bs i]
  (case (:type schema)
    :null [nil i]
    :boolean (b/boolean-at bs i)
    (:int :long) (b/long-at bs i)
    :float (b/float-at bs i)
    :double (b/double-at bs i)
    :bytes (b/bytes-at bs i)
    :string (b/string-at bs i)
    :enum (let [[idx i'] (b/long-at bs i)]
            [(nth (:symbols schema) (long idx)) i'])
    :fixed [(subvec (vec bs) i (+ i (:size schema))) (+ i (:size schema))]
    :union (let [[idx i'] (b/long-at bs i)
                 branch (nth (:branches schema) (long idx) nil)]
             (when-not branch
               (throw (ex-info "union branch index is outside the schema"
                               {:type :avro/malformed :index idx
                                :branches (count (:branches schema))})))
             (decode branch bs i'))
    :array (decode-array schema bs i)
    :map (decode-map schema bs i)
    :record (reduce (fn [[acc j] {:keys [name schema]}]
                      (let [[v j'] (decode schema bs j)]
                        [(assoc acc name v) j']))
                    [{} i] (:fields schema))
    (throw (ex-info (str "avro: decoding " (pr-str (:type schema))
                         " is not implemented")
                    {:type :avro/unsupported-type :schema (:type schema)}))))
