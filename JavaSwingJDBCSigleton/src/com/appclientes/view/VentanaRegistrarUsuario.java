/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.appclientes.view;



import com.appclientes.data.DataConnection;
import com.appclientes.model.Usuario;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.*;


/**
 * Clase JDialog con el formulario de registro de un usuario
 * @author scabrera
 */
public class VentanaRegistrarUsuario extends JDialog{
    
    private JLabel lblUsuario;
    private JLabel lblNombre;
    private JLabel lblClave;   
    private JLabel lblConfirClave;       

   
    
    private JTextField txtUsuario;
    private JTextField txtNombre;
    private JPasswordField txtClave;
    private JPasswordField txtConfClave;
  
    private JButton btnGuardar;
    
    
    
    public VentanaRegistrarUsuario(JFrame parent, boolean modal){
        configurarVentana(parent);
        inicializarComponentes();     
    }
        
        
    private void configurarVentana(JFrame parent) {
        this.setTitle("Registro de Usuario");
        this.setSize(450, 550);
        this.setLocationRelativeTo(parent);
        this.setLayout(null);
        this.setResizable(false);
        this.setModal(true);
        
    }

    private void inicializarComponentes() {
        // creamos los componentes
        lblUsuario = new JLabel("Usuario:");
        lblNombre = new JLabel("Nombre:");     
        lblClave = new JLabel("Clave:");     
        lblConfirClave = new JLabel("Confirmar Clave:");     
        
        txtUsuario = new JTextField();
        txtNombre = new JTextField(); 
        txtClave = new JPasswordField(); 
        txtConfClave = new JPasswordField(); 
        
        btnGuardar = new JButton("Guardar");        
                
        lblUsuario.setBounds(50, 50, 100, 25);
        txtUsuario.setBounds(150, 50, 100, 25);
        lblNombre.setBounds(50, 100, 100, 25);
        txtNombre.setBounds(150, 100, 200, 25);                
        lblClave.setBounds(50, 150, 100, 25);
        txtClave.setBounds(150, 150, 200, 25);   
        lblConfirClave.setBounds(50, 200, 100, 25);
        txtConfClave.setBounds(150, 200, 200, 25);                 
        btnGuardar.setBounds(100, 300, 150, 30);
                
        this.add(lblUsuario);  
        this.add(txtUsuario);   
        this.add(lblNombre);
        this.add(txtNombre);
        this.add(lblClave);
        this.add(txtClave);
        this.add(lblConfirClave);
        this.add(txtConfClave);       
        this.add(btnGuardar);
        
        
        
        // Agregamos escuchador a boton guardar
        Escuchador escucha = new Escuchador();
        btnGuardar.addActionListener(escucha);        
        
    }
   

    private class Escuchador implements ActionListener{
        @Override
        public void actionPerformed(ActionEvent e) {
            if(String.valueOf(txtClave.getPassword())
                    .equals(String.valueOf(
                            txtConfClave.getPassword()))){            
                crearUsuario();
            }else{
                JOptionPane.showMessageDialog(null, 
                    "Confirmación de clave no coincide.",
                    "Información!",
                    JOptionPane.INFORMATION_MESSAGE);  
            }
        }            
    }
    
    
    
    
    /**
     * Metodo para crear un usuario
     */
    public void crearUsuario() {     
        
        //obtenemos los valores de los componentes
        String usuario = txtUsuario.getText();
        String nombre = txtNombre.getText();
        String clave = String.valueOf(txtClave.getPassword());
        
        Usuario usuario1 = new Usuario(usuario, nombre, clave);
       

        int confirm = JOptionPane.showConfirmDialog(this,
            "¿Confirma que desea guardar este registro?",
            "Confirmar",
            JOptionPane.YES_NO_OPTION);
        if(confirm==0){    
            if(!usuario.equals("") && !nombre.equals("")
                && !clave.equals("")){                                    

                boolean result = DataConnection.getInstance().crearUsuario(usuario1);

                if(result){
                    dispose();
                    JOptionPane.showMessageDialog(this, "Guardado exitoso de usuario.",
                            "Exitoso!",JOptionPane.INFORMATION_MESSAGE);    // mostramos un mensaje (frame, mensaje)
                }else{
                    JOptionPane.showMessageDialog(this, "Error al realizar guardado de usuario."
                            + "{"+usuario+"}",
                            "Error",JOptionPane.ERROR_MESSAGE);    // mostramos un mensaje (frame, mensaje)                        
                }
            }else{
                JOptionPane.showMessageDialog(this, "Debe ingresar la información completa..",
                        "Información!",JOptionPane.INFORMATION_MESSAGE);    // mostramos un mensaje (frame, mensaje)                
            }
        }                      
    }   
    
     

    
}
