package com.yudachi.ct.web.Controller;

import com.yudachi.ct.web.Service.ViewService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class ViewController {

    @Autowired
    private ViewService viewService;

    @RequestMapping("/view")
    public String getViewJsp(){
        return "index";
    }

    @RequestMapping("/data")
    public Object view(){
        Map<String,String> map = new HashMap<>();
        map.put("username", "xll");
        map.put("age", "20");

        return map;
    }
}
