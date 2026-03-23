package com.oms.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class ProductResponse {

    private int productId;
    private String productName;
    private String productDescription;
    private BigDecimal price;
    private String category;

}
