/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.appclientes.view.main;

import com.appclientes.view.VentanaLogin;

/**
 * Clase que inicio de ejecucion de aplicacion, instancia la ventana de menu
 * @author scabrera
 */
public class Main {

    
    public static void main(String[] args) {
        VentanaLogin  login = new VentanaLogin();     
        login.setVisible(true);
    }    
    
}
