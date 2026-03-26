package com.oms.mapper;

import com.oms.dto.ProductRequestDTO;
import com.oms.dto.ProductResponseDTO;
import com.oms.entity.Product;

public class ProductMapper {

    public static Product toEntity(ProductRequestDTO request){
        Product product = new Product();
        product.setProductName(request.getProductName());
        product.setProductDescription(request.getDescription());
        product.setPrice(request.getPrice());
        product.setCategory(request.getCategory());
        return product;
    }

    public static ProductResponseDTO toResponse(Product product){
        return ProductResponseDTO.builder()
                .productId(product.getProductId())
                .productName(product.getProductName())
                .productDescription(product.getProductDescription())
                .price(product.getPrice())
                .category(product.getCategory())
                .build();
    }
}
