package com.oms.mapper;

import com.oms.dto.ProductRequest;
import com.oms.dto.ProductResponse;
import com.oms.entity.Product;

public class ProductMapper {

    public static Product toEntity(ProductRequest request){
        Product product = new Product();
        product.setProductName(request.getProductName());
        product.setProductDescription(request.getDescription());
        product.setPrice(request.getPrice());
        product.setCategory(request.getCategory());
        return product;
    }

    public static ProductResponse toResponse(Product product){
        return ProductResponse.builder()
                .productId(product.getProductId())
                .productName(product.getProductName())
                .productDescription(product.getProductDescription())
                .price(product.getPrice())
                .category(product.getCategory())
                .build();
    }

}
