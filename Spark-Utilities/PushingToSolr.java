// For each RDD
solrInputDocumentJavaDStream.foreachRDD(
        new Function<JavaRDD<SolrInputDocument>, Void>() {
          @Override
          public Void call(JavaRDD<SolrInputDocument> solrInputDocumentJavaRDD) throws Exception {

            // For each item in a single RDD
            solrInputDocumentJavaRDD.foreach(
                    new VoidFunction<SolrInputDocument>() {
                      @Override
                      public void call(SolrInputDocument solrInputDocument) {

                        // Add the solrInputDocument to the list of SolrInputDocuments
                        SolrIndexerDriver.solrInputDocumentList.add(solrInputDocument);
                      }
                    });

            // Try to force execution
            solrInputDocumentJavaRDD.collect();


            // After having finished adding every SolrInputDocument to the list
            // add it to the solrServer, and commit, waiting for the commit to be flushed
            try {

              // Seems like when run in cluster mode, the list size is zero,
             // therefore the following part is never executed

              if (SolrIndexerDriver.solrInputDocumentList != null
                      && SolrIndexerDriver.solrInputDocumentList.size() > 0) {
                SolrIndexerDriver.solrServer.add(SolrIndexerDriver.solrInputDocumentList);
                SolrIndexerDriver.solrServer.commit(true, true);
                SolrIndexerDriver.solrInputDocumentList.clear();
              }
            } catch (SolrServerException | IOException e) {
              e.printStackTrace();
            }


            return null;
          }
        }
);