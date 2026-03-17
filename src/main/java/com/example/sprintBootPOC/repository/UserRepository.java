package com.example.sprintBootPOC.repository;

import com.example.sprintBootPOC.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, Long> {
}
