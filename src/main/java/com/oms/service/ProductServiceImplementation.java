package com.oms.service;

import com.oms.dto.ProductRequest;
import com.oms.dto.ProductResponse;
import com.oms.entity.Product;
import com.oms.mapper.ProductMapper;
import com.oms.repository.ProductRepository;
import org.apache.coyote.BadRequestException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProductServiceImplementation implements ProductService{

    private final ProductRepository productRepository;

    ProductServiceImplementation(ProductRepository productrepo)
    {
        this.productRepository = productrepo;
    }

    @Override
    public ProductResponse createProduct(ProductRequest request) throws BadRequestException {
        if (request.getPrice() == null || request.getPrice().doubleValue() <= 0) {
            throw new BadRequestException("Price must be greater than zero");
        }

        if (request.getProductName() == null || request.getProductName().isBlank()) {
            throw new BadRequestException("Product name is required");
        }

        Product product = ProductMapper.toEntity(request);
        Product savedProduct = productRepository.save(product);
        return ProductMapper.toResponse(savedProduct);
    }

    @Override
    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }

    @Override
    public Product getProductById(int productId) {
        return productRepository.findById(productId).orElseThrow(() -> new RuntimeException(("Product not found")));
    }
}
