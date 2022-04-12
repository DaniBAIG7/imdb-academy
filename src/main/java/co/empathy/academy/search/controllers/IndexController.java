package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.empathy.academy.search.util.ClientCustomConfiguration;
import co.empathy.academy.search.util.TsvReader;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.util.List;

@RestController
public class IndexController {

    /**
     * This method answers a get petition to index the document. Firstly it creates an index, then it applies a
     * mapping an finally indexes all the documents contained in the films .tsv.
     */
    @GetMapping("/index_documents")
    public void indexDocuments() {
        try {

            ClientCustomConfiguration.getClient().indices().delete(i -> i.index("films")); //TODO delete this

            ClientCustomConfiguration.getClient().indices().create(i -> i.index("films"));

            ClientCustomConfiguration.getClient().indices().putMapping(_0 -> _0.index("films")
                    .properties("titleType", _1 -> _1
                            .text(_2 -> _2))
                    .properties("primaryTitle", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer("standard").fields("raw", _3 -> _3.keyword(_4 -> _4))
                            ))
                    .properties("originalTitle", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer("standard").fields("raw", _3 -> _3.keyword(_4 -> _4))
                            ))
                    .properties("isAdult", _1 -> _1
                            .boolean_(_2 -> _2))
                    .properties("startYear", _1 -> _1
                            .integer(_2 -> _2.index(false)))
                    .properties("endYear", _1 -> _1
                            .integer(_2 -> _2))
                    .properties("runtimeMinutes", _1 -> _1
                            .integer(_2 -> _2.index(false)))
                    .properties("genres", _1 -> _1
                            .text(_2 -> _2.analyzer("english")))
            );

            TsvReader r = new TsvReader();

            while (r.isOpen()) {//The custom TSVReader has not read a

                var a = ClientCustomConfiguration.getClient().bulk(_0 -> _0
                        .operations(r.readSeveral().entrySet().stream().map(_1 -> BulkOperation.of(
                                _2 -> _2.index(_3 -> _3.index("films")
                                        .id(_1.getKey())
                                        .document(_1.getValue()))
                        )).toList()));


                    if(a.errors()) {
                      for(var d: a.items()) {
                        if(d.error() != null)
                            System.out.println(d.error().reason());
                    }
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
