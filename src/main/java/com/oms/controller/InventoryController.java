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
@RequestMapping("/inventory")
public class InventoryController {

    private final InventoryService inventoryService;

    // ✅ Constructor Injection (BEST PRACTICE)
    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    // ✅ 1. Add / Update Inventory
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public InventoryResponseDTO addOrUpdateInventory(
            @Valid @RequestBody InventoryRequestDTO request) {

        return inventoryService.addOrUpdateInventory(request);
    }

    // ✅ 2. Get Product Availability
    @GetMapping("/product/{productId}")
    public List<InventoryResponseDTO> getProductAvailability(
            @PathVariable int productId) {

        return inventoryService.getProductAvailability(productId);
    }

    // ✅ 3. Reduce Inventory (after order)
    @PostMapping("/reduce")
    @ResponseStatus(HttpStatus.OK)
    public InventoryResponseDTO reduceInventory(
            @Valid @RequestBody InventoryRequestDTO request) {

        return inventoryService.reduceInventory(
                request.getProductId(),
                request.getWarehouseId(),
                request.getQuantity()
        );
    }

    // ✅ 4. Restore Inventory (when order is cancelled)
    @PostMapping("/restore")
    @ResponseStatus(HttpStatus.OK)
    public InventoryResponseDTO restoreInventory(
            @Valid @RequestBody InventoryRequestDTO request) {

        return inventoryService.restoreInventory(
                request.getProductId(),
                request.getWarehouseId(),
                request.getQuantity()
        );
    }

    @GetMapping("/availability")
    public boolean isProductAvailable(
            @RequestParam int productId,
            @RequestParam int warehouseId,
            @RequestParam int quantity) {

        return inventoryService.isProductAvailable(productId, warehouseId, quantity);
    }

}
