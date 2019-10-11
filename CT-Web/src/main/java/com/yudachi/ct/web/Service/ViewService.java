package com.yudachi.ct.web.Service;

import com.yudachi.ct.web.Repository.CalllogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ViewService {

    @Autowired
    private CalllogRepository calllogRepository;

}
