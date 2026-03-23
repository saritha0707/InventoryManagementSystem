package com.oms.controller;
import java.util.List;

import com.oms.dto.ProductRequest;
import com.oms.dto.ProductResponse;
import com.oms.entity.Product;
import com.oms.service.ProductService;
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
    public ProductResponse createProduct(@RequestBody ProductRequest request){
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
