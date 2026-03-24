package com.oms.repository;

import com.oms.entity.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface InventoryRepository extends JpaRepository<Inventory,Integer> {

    Optional<Inventory> findByProduct_ProductIdAndWarehouse_WarehouseId(int productId,int warehouseId);

    List<Inventory> findByProduct_ProductId(int productId);

    List<Inventory> findByWarehouse_WarehouseId(int warehouseId);
}
