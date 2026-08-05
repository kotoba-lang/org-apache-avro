(ns avro.reader-test
  "Decoded against files written by Avro's own writer.

  Every fixture came out of fastavro, and so did every expectation:
  `ground-truth.edn` is emitted by the same script from the files it just
  wrote. That matters more for Avro than for the columnar formats, because
  **Avro writes no type tags** — a decoder that is wrong about the schema
  produces plausible values rather than an error, so a self-generated fixture
  would agree with a wrong decoder."
  (:require [avro.file :as file]
            [avro.schema :as schema]
            [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            #?(:clj [clojure.java.io :as io])))

(defn read-fixture [name]
  #?(:clj (with-open [in (io/input-stream (io/file "test/fixtures" name))]
            (let [out (java.io.ByteArrayOutputStream.)]
              (io/copy in out)
              (mapv #(bit-and % 0xff) (.toByteArray out))))
     :cljs (vec (js/Array.from (.readFileSync (js/require "fs")
                                              (str "test/fixtures/" name))))))

(defn- read-text [name]
  #?(:clj (slurp (io/file "test/fixtures" name))
     :cljs (.readFileSync (js/require "fs") (str "test/fixtures/" name) "utf8")))

(def plain (delay (read-fixture "plain.avro")))
(def deflated (delay (read-fixture "deflate.avro")))
(def zstded (delay (read-fixture "zstd.avro")))
(def multi (delay (read-fixture "multi-block.avro")))
(def snappied (delay (read-fixture "snappy.avro")))
(def bigints (delay (read-fixture "big-ints.avro")))
(def truth (delay (edn/read-string (read-text "ground-truth.edn"))))


;; ── the schema travels with the file ────────────────────────────────────────

(deftest the-file-states-its-own-schema
  ;; Avro's real distinguishing feature: unlike CSV, no declared schema from
  ;; the caller is needed, and none can disagree with the bytes.
  (let [s (file/file-schema @plain)]
    (is (= :record (:type s)))
    (is (= "Sale" (:name s)))
    (is (= (get @truth "fields") (schema/record-fields s)))))

(deftest a-nullable-union-is-the-ordinary-case
  (let [s (file/file-schema @plain)
        note (first (filter #(= "note" (:name %)) (:fields s)))]
    (is (= :union (:type (:schema note))))
    (is (= [:null :string] (mapv :type (:branches (:schema note)))))))

;; ── values ──────────────────────────────────────────────────────────────────

(deftest records-decode-to-what-was-written
  (let [got (file/records @plain)]
    (is (= 3 (count got)))
    (is (= (get @truth "rows") got))))

(deftest a-value-past-2-to-the-53-is-exact-or-refused
  ;; Its own fixture, because Avro decodes a RECORD AT A TIME: one field out of
  ;; range makes the whole record undecodable, not just that field. Parquet and
  ;; Arrow read columns independently, so there only the offending column
  ;; refuses. That difference is a property of row-orientation, and keeping the
  ;; big value in the main table would have made every value assertion here
  ;; unrunnable under ClojureScript rather than just this one.
  #?(:clj (is (= (get @truth "big") (mapv #(get % "big") (file/records @bigints))))
     :cljs (is (thrown? :default (file/records @bigints))))
  (testing "and the record count is readable either way, because block headers
            are not records"
    (is (= 3 (file/record-count @bigints)))))

(deftest a-null-branch-of-a-union-is-nil-not-missing
  (let [got (file/records @plain)]
    (is (contains? (first got) "note"))
    (is (nil? (get (first got) "note")))
    (is (= "clearance" (get (second got) "note")))))

;; ── codecs ──────────────────────────────────────────────────────────────────

(deftest deflate-agrees-with-plain
  ;; Avro's `deflate` is RAW deflate with no gzip header, so this goes through
  ;; inflate-raw rather than gunzip. Getting it wrong fails on the first byte,
  ;; which is the good case.
  (is (= (file/records @plain)
         (file/records @deflated))))

(deftest zstd-agrees-with-plain
  (is (= (file/records @plain)
         (file/records @zstded))))

(deftest several-blocks-decode-as-one-sequence
  ;; A single-block file cannot tell a correct block walk from one that never
  ;; moved, so the fixture forces several with a tiny sync interval.
  (is (< 1 (count (file/blocks @multi))))
  (is (= (file/records @plain)
         (file/records @multi))))

;; ── the property a partial reader earns its keep with ───────────────────────

(deftest the-record-count-survives-a-codec-this-reader-cannot-undo
  ;; The Avro analogue of Parquet statistics being readable from a file whose
  ;; pages are not. Block headers carry (count, byte-size) and are never
  ;; compressed, so the walk succeeds on a file the decoder refuses.
  (is (= 3 (file/record-count @snappied)))
  (is (= 1 (count (file/blocks @snappied)))
      "one block holding three records -- so the count came from the block
       HEADER rather than from counting blocks, which is the point")
  (testing "and the schema is readable too"
    (is (= (get @truth "fields") (schema/record-fields (file/file-schema @snappied)))))
  (testing "while decoding refuses and names the codec"
    (let [e (try (file/records @snappied) nil
                 (catch #?(:clj Exception :cljs :default) e (ex-data e)))]
      (is (= :avro/unsupported-codec (:type e)))
      (is (= "snappy" (:codec e)))
      (is (= (set file/decodable-codecs) (set (:decodable e)))
          "the refusal names what IS decodable, so it stays accurate as codecs
           land rather than drifting into a lie"))))

(deftest the-record-count-agrees-with-the-records
  (doseq [[nm bs] [["plain" @plain] ["deflate" @deflated]
                   ["zstd" @zstded] ["multi-block" @multi]]]
    (is (= (file/record-count bs) (count (file/records bs))) nm)))

;; ── refusals ────────────────────────────────────────────────────────────────

(deftest a-foreign-file-is-refused
  (is (thrown? #?(:clj Exception :cljs :default) (file/blocks (vec (repeat 32 0))))))

(deftest a-corrupted-sync-marker-is-caught
  ;; Skipping the check would walk off the end of a truncated block and decode
  ;; the next block's length as a record -- producing values, which is the
  ;; failure worth one comparison per block to avoid.
  (let [bs @plain
        ;; The last 16 bytes of the file are the final block's sync marker.
        broken (assoc (vec bs) (- (count bs) 1) (bit-xor 0xFF (nth bs (dec (count bs)))))
        e (try (file/blocks broken) nil
               (catch #?(:clj Exception :cljs :default) e (ex-data e)))]
    (is (= :avro/desynchronised (:type e)))))

(deftest an-unknown-named-type-is-refused-rather-than-guessed
  ;; A schema referencing an undefined type would otherwise decode as though
  ;; the field were a string, and consume the wrong bytes.
  (let [e (try (schema/parse "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"x\",\"type\":\"Nope\"}]}")
               nil
               (catch #?(:clj Exception :cljs :default) e (ex-data e)))]
    (is (= :avro/unknown-type (:type e)))
    (is (= "Nope" (:name e)))))

(deftest a-self-referential-record-terminates
  ;; Avro allows it and a linked list is the ordinary example. The record is
  ;; registered before its fields resolve, so this does not recurse forever.
  (let [s (schema/parse (str "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
                             "{\"name\":\"next\",\"type\":[\"null\",\"Node\"]}]}"))]
    (is (= :record (:type s)))
    (is (= ["next"] (schema/record-fields s)))))
