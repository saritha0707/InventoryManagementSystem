package com.oms.service;

import com.oms.entity.Product;
import com.oms.dto.ProductRequest;
import com.oms.dto.ProductResponse;

import java.util.List;

public interface ProductService {

    ProductResponse createProduct(ProductRequest request);
    List<Product> getAllProducts();
    Product getProductById(int productId);
}
