;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.js
  (:refer-clojure :exclude [])
  (:require-macros
     [datomish.pair-chan :refer [go-pair <?]])
  (:require
     [cljs.core.async :as a :refer [take! <! >!]]
     [cljs.reader]
     [cljs-promises.core :refer [promise]]
     [datomish.db :as db]
     [datomish.db-factory :as db-factory]
     [datomish.pair-chan]
     [datomish.sqlite :as sqlite]
     [datomish.simple-schema :as simple-schema]
     [datomish.js-sqlite :as js-sqlite]
     [datomish.transact :as transact]))

(defn- take-pair-as-promise! [ch f]
  ;; Just like take-as-promise!, but aware that it's handling a pair channel.
  ;; Also converts values, if desired.
  (promise
    (fn [resolve reject]
      (letfn [(split-pair [[v e]]
                (if e
                  (reject e)
                  (resolve (f v))))]
        (cljs.core.async/take! ch split-pair)))))

;; Public API.

(defn ^:export db [conn]
  (transact/db conn))

(defn ^:export q [db find options]
  (let [find (cljs.reader/read-string find)
        options (js->clj options)]
    (take-pair-as-promise!
      (db/<?q db find options)
      clj->js)))

(defn ^:export ensure-schema [conn simple-schema]
  (let [simple-schema (js->clj simple-schema :keywordize-keys true)]
    (take-pair-as-promise!
      (transact/<transact!
        conn
        (simple-schema/simple-schema->schema simple-schema))
      clj->js)))

(defn js->tx-data [tx-data]
  ;; Objects to maps.
  ;; Arrays to arrays.
  ;; RHS strings… well, some of them will be richer types.
  ;; TODO
  (js->clj tx-data :keywordize-keys true))

(defn ^:export transact [conn tx-data]
  ;; Expects a JS array as input.
  (let [tx-data (js->tx-data tx-data)]
    (take-pair-as-promise!
      (go-pair
        (let [tx-result (<? (transact/<transact! conn tx-data))]
          (select-keys tx-result
                       [:tempids
                        :added-idents
                        :added-attributes
                        :tx
                        :txInstant])))
      clj->js)))

(defn ^:export open [path]
  ;; Eventually, URI.  For now, just a plain path (no file://).
  (take-pair-as-promise!
    (go-pair
      (let [conn (<? (sqlite/<sqlite-connection path))
            db (<? (db-factory/<db-with-sqlite-connection conn))]
        (let [c (transact/connection-with-db db)]
        (clj->js
          {:conn c
           :ensureSchema (fn [simple-schema] (ensure-schema c simple-schema))
           :transact (fn [tx-data] (transact c tx-data))
           :q (fn [find options] (q (transact/db c) find options))
           :close (fn [] (db/close-db db))
           :toString (fn [] (str "#<DB " path ">"))
           :path path}))))
    identity))
