package com.oms.controller;
import java.util.List;

import com.oms.dto.ProductRequest;
import com.oms.dto.ProductResponse;
import com.oms.entity.Product;
import com.oms.service.ProductService;
import jakarta.validation.Valid;
import org.apache.coyote.BadRequestException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/oms/products")
public class ProductController {
    private final ProductService productService;

    public ProductController(ProductService productService){
        this.productService = productService;
    }

    //Add Product
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ProductResponse createProduct(@Valid @RequestBody ProductRequest request) throws BadRequestException {
        ProductResponse response = productService.createProduct(request);
        return response;
    }

    //Get All Products
    @GetMapping
    public List<Product> getAllProducts(){
        return productService.getAllProducts();
    }

    //Get Product by ID
    @GetMapping("/{productId}")
    public Product getByProductId(@PathVariable int productId){
        return productService.getProductById(productId);
    }
}
