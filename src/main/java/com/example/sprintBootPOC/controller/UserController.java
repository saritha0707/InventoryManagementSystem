package com.example.sprintBootPOC.controller;

import com.example.sprintBootPOC.entity.User;
import com.example.sprintBootPOC.service.UserService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/users")
public class UserController {
    private final UserService service;

    public UserController(UserService service) {
        this.service = service;
    }

    @PostMapping
    public User createUser(@RequestBody User user) {
        return service.createUser(user);
    }

    @GetMapping
    public List<User> getUsers() {
        return service.getUsers();
    }
}
