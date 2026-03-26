package com.oms.service;

import com.oms.entity.Product;
import com.oms.dto.ProductRequestDTO;
import com.oms.dto.ProductResponseDTO;
import org.apache.coyote.BadRequestException;

import java.util.List;

public interface ProductService {

    ProductResponseDTO createProduct(ProductRequestDTO request) throws BadRequestException;
    List<ProductResponseDTO> getAllProducts();
    ProductResponseDTO getProductById(int productId);
}
