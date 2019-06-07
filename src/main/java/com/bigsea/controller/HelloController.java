package com.bigsea.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = {"/hello"})
public class HelloController {

    @RequestMapping(value = {"/springboot.action"})
    public String hello() {
        return "helloSpringBoot";
    }

    public void test() {
        System.out.println("测试");
    }
}
