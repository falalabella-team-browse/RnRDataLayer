package com.hachathon.reviewNratings.controller;

import com.hachathon.reviewNratings.service.ReviewRatingsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.http.HttpResponse;

@RestController()
public class WriteController {

    @Autowired
    public ReviewRatingsService reviewRatingsService;

    @GetMapping(value = "/search")
    public String bulkIngest(){
        reviewRatingsService.bulkIngest();
        return "success";
    }

}
