/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.appclientes.model;

/**
 *
 * @author scabrera
 */
public class Sucursal {
 
    private Proveedor proveedor;
    private String sucursal;

    public Sucursal() {
    }

    public Sucursal(Proveedor proveedor, String sucursal) {
        this.proveedor = proveedor;
        this.sucursal = sucursal;
    }

    public Proveedor getProveedor() {
        return proveedor;
    }

    public void setProveedor(Proveedor proveedor) {
        this.proveedor = proveedor;
    }

    public String getSucursal() {
        return sucursal;
    }

    public void setSucursal(String sucursal) {
        this.sucursal = sucursal;
    }

    @Override
    public String toString() {
        return "Sucursal{" + "proveedor=" + proveedor.getNombre() + ", sucursal=" + sucursal + '}';
    }
    
    
    
    
    
}
