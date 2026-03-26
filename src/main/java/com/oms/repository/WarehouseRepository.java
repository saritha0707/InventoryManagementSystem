package com.oms.repository;

import com.oms.entity.Warehouse;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface WarehouseRepository extends JpaRepository<Warehouse, Integer> {
    @Query("SELECT w.warehouseId FROM Warehouse w WHERE w.warehouseName = :name")
    int findWarehouseIdByName(@Param("name") String name);
}
