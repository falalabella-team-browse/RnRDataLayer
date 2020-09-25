package com.hachathon.reviewNratings.repository;

import java.util.List;


import com.hachathon.reviewNratings.models.Customer;
import org.springframework.data.repository.CrudRepository;

public interface CustomerRepository extends CrudRepository<Customer, Long>{
    List<Customer> findByFirstName(String FirstName);
    List<Customer> findAll();
}
