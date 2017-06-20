/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.appclientes.view;

import com.appclientes.data.DataConnection;
import com.appclientes.model.Usuario;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.net.URL;
import java.util.Arrays;
import javax.swing.*;

/**
 *
 * @author scabrera
 */
public class VentanaLogin extends JFrame{
    
    private JLabel userLabel;
    private JTextField userText;
    private JLabel passwordLabel;
    private JPasswordField passwordText;
    private JButton loginButton;
    private JButton cancelButton;
    private JLabel lblImagenLogin;
    
     public VentanaLogin() {
        configurarComponentes();
        configurarVentana();
    }

    private void configurarVentana(){
        this.setTitle("Login");
        this.setSize(400, 150);
        this.setLocationRelativeTo(null);
        this.setLayout(null);           
        this.setResizable(false);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }
     
    private void configurarComponentes() {



        userLabel = new JLabel("Usuario");
        userLabel.setBounds(110, 10, 80, 25);
        this.add(userLabel);

        userText = new JTextField(20);
        userText.setBounds(200, 10, 160, 25);
        this.add(userText);

        passwordLabel = new JLabel("Contraseña");
        passwordLabel.setBounds(110, 40, 80, 25);
        this.add(passwordLabel);

        passwordText = new JPasswordField(20);
        passwordText.setBounds(200, 40, 160, 25);
        this.add(passwordText);
        
        loginButton = new JButton("Ingresar");
        loginButton.setBounds(110, 80, 100, 25);
        this.add(loginButton);

        cancelButton = new JButton("Cancelar");
        cancelButton.setBounds(260, 80, 100, 25);
        this.add(cancelButton);
        
        lblImagenLogin=new JLabel();
        lblImagenLogin.setBounds(10,10,80,80);       
        URL url = this.getClass().
                getResource("/resources/Login.gif");
        ImageIcon fot = new ImageIcon(url);
        Icon icono = new ImageIcon(fot.getImage()
                .getScaledInstance(
                        lblImagenLogin.getWidth(),
                        lblImagenLogin.getHeight(),
                        Image.SCALE_DEFAULT));
        lblImagenLogin.setIcon(icono);
        this.repaint();
        this.add(lblImagenLogin);        
        
        Escuchador listener = new Escuchador();
        loginButton.setActionCommand("EventoIngresar");
        loginButton.addActionListener(listener);
        
        cancelButton.setActionCommand("EventoCancelar");
        cancelButton.addActionListener(listener);        
        
    }
    
    
    public class Escuchador implements ActionListener{

        @Override
        public void actionPerformed(ActionEvent e) {
            switch(e.getActionCommand()){
                case "EventoCancelar":
                    System.exit(0);
                case "EventoIngresar":
                    if(userText.getText()!=null && passwordText.getPassword()!=null){
                        boolean result =validarUsuario(userText.getText(), passwordText.getPassword());
                        if(result){
                            dispose();
                            VentanaMenu menu = new VentanaMenu();
                            menu.showMenu();
                        }else{
                            JOptionPane.showMessageDialog(null, "Usuario no valido.",
                                "Error!",JOptionPane.ERROR_MESSAGE);                          
                        }
                    }
            }
        }
    }
    
    private boolean validarUsuario(String Idusuario, char[] clave){
        Usuario objUsuario = DataConnection.getInstance().buscarUsuario(Idusuario);
        if(objUsuario!=null){
            if(objUsuario.getClave().equals(String.valueOf(clave)) ){
                return true;
            }
        }
        
        return false;
    }
    
    

}