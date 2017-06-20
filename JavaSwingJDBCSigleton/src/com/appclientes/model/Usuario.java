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
public class Usuario {
    
    private String Usuario;
    private String Nombre;
    private String Clave;

    public Usuario() {
    }

    
    
    public Usuario(String Usuario, String Nombre, String Clave) {
        this.Usuario = Usuario;
        this.Nombre = Nombre;
        this.Clave = Clave;
    }

    public String getUsuario() {
        return Usuario;
    }

    public void setUsuario(String Usuario) {
        this.Usuario = Usuario;
    }

    public String getNombre() {
        return Nombre;
    }

    public void setNombre(String Nombre) {
        this.Nombre = Nombre;
    }

    public String getClave() {
        return Clave;
    }

    public void setClave(String Clave) {
        this.Clave = Clave;
    }

    @Override
    public String toString() {
        return "Usuario{" + "Usuario=" + Usuario + ", Nombre=" + Nombre + ", Clave=" + Clave + '}';
    }
    

    
    
}
