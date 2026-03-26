package com.oms.controller;

import com.oms.dto.InventoryRequestDTO;
import com.oms.dto.InventoryResponseDTO;
import com.oms.service.InventoryService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/oms/inventory")
public class InventoryController {


    @Autowired
    private InventoryService inventoryService;

//    // Constructor Injection (same as ProductController)
//    public InventoryController(InventoryService inventoryService) {
//        this.inventoryService = inventoryService;
//    }

    //  Add / Update Inventory
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public InventoryResponseDTO addOrUpdateInventory(@Valid @RequestBody InventoryRequestDTO request) {

        return inventoryService.addOrUpdateInventory(request);
    }

    // ✅ Service 1: Get product availability (from Product + Warehouse + Inventory)
    @GetMapping("/product/{productId}")
    public List<InventoryResponseDTO> getProductAvailability(@PathVariable int productId) {
        return inventoryService.getProductAvailability(productId);
    }

    // ✅ Service 2: Reduce inventory after order
    @PostMapping("/reduce")
    @ResponseStatus(HttpStatus.OK)
    public InventoryResponseDTO reduceInventory(@Valid @RequestBody InventoryRequestDTO request) {

//        @RequestParam int productId,
//        @RequestParam int warehouseId,
//        @RequestParam int quantity

        return inventoryService.reduceInventory(
                request.getProductId(),
                request.getWarehouseId(),
                request.getQuantity()
        );
    }
}