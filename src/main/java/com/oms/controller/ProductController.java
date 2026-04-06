package com.oms.controller;
import java.util.List;

import com.oms.dto.ProductRequestDTO;
import com.oms.dto.ProductResponseDTO;
import com.oms.entity.Product;
import com.oms.exception.BadRequestException;
import com.oms.service.ProductService;
import io.swagger.v3.oas.annotations.Hidden;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/products")
public class ProductController {
    private final ProductService productService;

    public ProductController(ProductService productService){
        this.productService = productService;
    }

    //Add Product
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ProductResponseDTO createProduct(@Valid @RequestBody ProductRequestDTO request) throws BadRequestException, org.apache.coyote.BadRequestException {
        return productService.createProduct(request);
    }

    //Get All Products
    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public List<ProductResponseDTO> getAllProducts(){
        return productService.getAllProducts();
    }

    //Get Product by ID
    @GetMapping("/{productId}")
    public ProductResponseDTO getByProductId(@PathVariable int productId){
        return productService.getProductById(productId);
    }
}
