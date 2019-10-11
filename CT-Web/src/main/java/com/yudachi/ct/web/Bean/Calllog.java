package com.yudachi.ct.web.Bean;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Entity
@Table(name = "tb_call")
public class Calllog{

    @Id
    private int id;

    private int telid;
    private int dateid;
    private int sumcall;
    private int sumduration;

}
