package com.oms.controller;

import com.oms.dto.InventoryResponse;
import com.oms.service.InventoryService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/oms/inventory")
public class InventoryController {

    private final InventoryService inventoryService;

    // Constructor Injection (same as ProductController)
    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    // ✅ Service 1: Get product availability (from Product + Warehouse + Inventory)
    @GetMapping("/product/{productId}")
    public List<InventoryResponse> getProductAvailability(@PathVariable int productId) {
        return inventoryService.getProductAvailability(productId);
    }

    // ✅ Service 2: Reduce inventory after order
    @PostMapping("/reduce")
    @ResponseStatus(HttpStatus.OK)
    public String reduceInventory(@RequestParam int productId,
                                  @RequestParam int warehouseId,
                                  @RequestParam int quantity) {

        inventoryService.reduceInventory(productId, warehouseId, quantity);
        return "Inventory updated successfully";
    }
}