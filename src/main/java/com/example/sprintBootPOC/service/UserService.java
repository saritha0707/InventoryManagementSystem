package com.example.sprintBootPOC.service;

import com.example.sprintBootPOC.entity.User;
import com.example.sprintBootPOC.repository.UserRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserService {

    private final UserRepository repository;

    public UserService(UserRepository repository){
        this.repository = repository;
    }

    public User createUser(User user){
        return repository.save(user);
    }

    public List<User> getUsers(){
        return repository.findAll();
    }
}
