package com.bigsea.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document(indexName = "mountain", type = "something", shards = 6, replicas = 1)
public class Something {
    @Id
    private Long id;

    @Field
    private String title;

    @Field
    private String price;
}
