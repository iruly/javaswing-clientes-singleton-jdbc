/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.appclientes.model;

import java.util.Date;

/**
 * Clase que representa la entidad cliente de los proyectos
 * @author scabrera
 */
public class Cliente {
    

    private int codigo;
    private String nit;
    private String email;
    private String pais;
    private Date fechaRegistro;
    private String razonSocial;
    private String idioma;
    private String categoria;

    /**
     * Constructor sin parametros
     */
    public Cliente() {
    }

    /**
     * Constructor con parametros
     * @param codigo
     * @param nit
     * @param email
     * @param pais
     * @param fechaRegistro
     * @param razonSocial
     * @param idioma
     * @param categoria 
     */
    public Cliente(int codigo, String nit, String email, String pais, Date fechaRegistro, String razonSocial, String idioma, String categoria) {
        this.codigo = codigo;
        this.nit = nit;
        this.email = email;
        this.pais = pais;
        this.fechaRegistro = fechaRegistro;
        this.razonSocial = razonSocial;
        this.idioma = idioma;
        this.categoria = categoria;
    }

    public Cliente(String nit, String email, String pais, Date fechaRegistro, String razonSocial, String idioma, String categoria) {
        this.nit = nit;
        this.email = email;
        this.pais = pais;
        this.fechaRegistro = fechaRegistro;
        this.razonSocial = razonSocial;
        this.idioma = idioma;
        this.categoria = categoria;
    }
    
    
    

    /** Modificadores **/
    public void setCodigo(int codigo) {
        this.codigo = codigo;
    } 
    
    public void setNit(String nit) {
        this.nit = nit;
    }    
    
    public void setEmail(String email) {
        this.email = email;
    }    
    
    public void setPais(String pais) {
        this.pais = pais;
    }    
    
    public void setFechaRegistro(Date fechaRegistro) {
        this.fechaRegistro = fechaRegistro;
    }    
    
    public void setRazonSocial(String razonSocial) {
        this.razonSocial = razonSocial;
    }    
    
    public void setIdioma(String idioma) {
        this.idioma = idioma;
    }
    
    public void setCategoria(String categoria) {
        this.categoria = categoria;
    }    
    
    
    
    /** Analizadores **/
    
    public int getCodigo() {
        return codigo;
    }

              
    public String getNit() {
        return nit;
    }


    public String getEmail() {
        return email;
    }


    public String getPais() {
        return pais;
    }


    public Date getFechaRegistro() {
        return fechaRegistro;
    }

    public String getRazonSocial() {
        return razonSocial;
    }


    public String getIdioma() {
        return idioma;
    }


    public String getCategoria() {
        return categoria;
    }

    @Override
    public String toString() {
        return "Cliente{" + "codigo=" + codigo + ", nit=" + nit + ", email=" + email + ", pais=" + pais + ", fechaRegistro=" + fechaRegistro + ", razonSocial=" + razonSocial + ", idioma=" + idioma + ", categoria=" + categoria + '}';
    }
   
        
}
