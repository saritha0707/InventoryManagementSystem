package com.oms.repository;

import com.oms.entity.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface InventoryRepository extends JpaRepository<Inventory,Long> {

    Optional<Inventory> findByProduct_ProductIdAndWarehouse_WarehouseId(Long productId,Long warehouseId);

    List<Inventory> findByProduct_ProductId(Long productId);

    List<Inventory> findByWarehouse_WarehouseId(Long warehouseId);
}
