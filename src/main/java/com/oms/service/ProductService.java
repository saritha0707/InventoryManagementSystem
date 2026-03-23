package com.oms.service;

import com.oms.entity.Product;
import com.oms.dto.ProductRequest;
import com.oms.dto.ProductResponse;
import org.apache.coyote.BadRequestException;

import java.util.List;

public interface ProductService {

    ProductResponse createProduct(ProductRequest request) throws BadRequestException;
    List<Product> getAllProducts();
    Product getProductById(int productId);
}
