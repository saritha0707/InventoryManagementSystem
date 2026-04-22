package com.oms.service;
import com.oms.dto.ProductRequestDTO;
import com.oms.dto.ProductResponseDTO;
import com.oms.entity.Product;
import com.oms.exception.ResourceNotFoundException;
import com.oms.mapper.ProductMapper;
import com.oms.repository.ProductRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import java.util.List;
@Slf4j
@Service
public class ProductServiceImplementation implements ProductService{

    private final ProductRepository productRepository;

    ProductServiceImplementation(ProductRepository productrepo)
    {
        this.productRepository = productrepo;
    }

    @Override
    public ProductResponseDTO createProduct(ProductRequestDTO request)  {
        try {
            log.info("Creating product: {}", request.getProductName());

            Product product = ProductMapper.toEntity(request);

            Product savedProduct = productRepository.save(product);

            log.info("Product created with id={}", savedProduct.getProductId());

            return ProductMapper.toResponse(savedProduct);
        }
        catch (DataIntegrityViolationException ex) {
            log.error("Database constraint violation while creating product: {}", request.getProductName(), ex);
            throw new RuntimeException("Product creation failed due to invalid data or duplicate entry");

        } catch (Exception ex) {
            log.error("Unexpected error while creating product: {}", request.getProductName(), ex);
            throw new RuntimeException("Something went wrong while creating product");
        }
    }

    @Override
    public List<ProductResponseDTO> getAllProducts() {
        log.info("Fetching all products");
        List<Product> products = productRepository.findAll();
        return products.stream()
                .map(ProductMapper::toResponse)
                .toList();

    }

    @Override
    public ProductResponseDTO getProductById(int productId) {
        log.info("Fetching product with id={}", productId);

        Product product = productRepository.findById(productId)
                .orElseThrow(() -> {
                    log.error("Product not found with id={}", productId);
                    return new ResourceNotFoundException("Product not found: " + productId);
                });

        return ProductMapper.toResponse(product);
    }
}
