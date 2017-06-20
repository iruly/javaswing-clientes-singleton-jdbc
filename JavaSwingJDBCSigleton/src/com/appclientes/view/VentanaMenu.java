/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.appclientes.view;


import com.appclientes.data.DataConnection;
import java.awt.*;
import java.awt.event.*;
import java.net.URL;
import javax.swing.*;

/**
 * Ventana de menu de la aplicacion
 * @author scabrera
 */
public class VentanaMenu{
    
   private JFrame mainFrame;
   private JLabel headerLabel;
   private JPanel controlPanel; 
   private JLabel lblImageMenu;

   public VentanaMenu(){
      configVen();
   }
      

   private void configVen(){
       
      mainFrame = new JFrame();

      //Se configura icono de la ventana

      ImageIcon ImageIcon = new ImageIcon
        (getClass().getResource
        ("/resources/icono.png"));
      Image Image = ImageIcon.getImage();
      mainFrame.setIconImage(Image);  
      
      mainFrame.setTitle("Administracion de clientes");         // colocamos titulo a la ventana
      mainFrame.setSize(900, 750);                               // colocamos tamanio a la ventana (ancho, alto)
      mainFrame.setResizable(false);                            // hacemos que la ventana no sea redimiensionable
      mainFrame.setLocationRelativeTo(null);                    // centramos la ventana en la pantalla
      //mainFrame.setLayout(new GridLayout(3, 1));
      mainFrame.setLayout(new BoxLayout(mainFrame.getContentPane(),BoxLayout.Y_AXIS));
      mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);    // hacemos que cuando se cierre la ventana termina todo proceso             
      
      /* configuramos label central de la aplicacion */
      headerLabel = new JLabel("Sistema de Administración de Clientes",JLabel.CENTER);
      Font fuente=new Font("Monospaced", Font.BOLD + Font.ITALIC, 30);      
      headerLabel.setFont(fuente);
      headerLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
      
      //Se configura imagen de centro del menu
      lblImageMenu=new JLabel();  
      URL url = this.getClass().
          getResource("/resources/servicio.png");
      ImageIcon fot = new ImageIcon(url);
      Icon icono = new ImageIcon(fot.getImage()
              .getScaledInstance(220, 220, 
                      Image.SCALE_DEFAULT));
      lblImageMenu.setIcon(icono);
      mainFrame.repaint();
      lblImageMenu.setAlignmentX(Component.CENTER_ALIGNMENT);
      mainFrame.add(lblImageMenu);  
      
      
      
      controlPanel = new JPanel();
      controlPanel.setLayout(new FlowLayout());

      /* agregamos componentes al frame */
      mainFrame.add(headerLabel);
      mainFrame.add(controlPanel);
  
      
   }
   
   
   public void showMenu(){
      //create a menu bar
      final JMenuBar menuBar = new JMenuBar();

      //create menus
      JMenu fileMenu = new JMenu("Clientes");
      JMenu fileMenu1 = new JMenu("Proveedores");
      JMenu fileMenu2 = new JMenu("Usuarios");
      JMenu fileMenu3 = new JMenu("Salir");
     
      //create menu items
      JMenuItem newMenuItem = new JMenuItem("Nuevo Cliente");
      newMenuItem.setActionCommand("NuevoCliente");

      JMenuItem openMenuItem = new JMenuItem("Editar Cliente");
      openMenuItem.setActionCommand("EditarCliente");

      JMenuItem listaMenuItem = new JMenuItem("Listar Clientes");
      listaMenuItem.setActionCommand("ListarClientes");
      
      //Opciones de cliente
      JMenuItem newUserMenuItem = new JMenuItem("Nuevo Usuario");
      newUserMenuItem.setActionCommand("NuevoUsuario");      

      //Opciones de Provedor
      JMenuItem newProvMenuItem = new JMenuItem("Nuevo Provedor");
      newProvMenuItem.setActionCommand("NuevoProveedor");

      JMenuItem newSalirMenuItem = new JMenuItem("Salida segura");
      newSalirMenuItem.setActionCommand("Salir");
      
      
      /* asociamos eventos de accion de items */
      MenuItemListener menuItemListener = new MenuItemListener();
      newMenuItem.addActionListener(menuItemListener);
      openMenuItem.addActionListener(menuItemListener);
      listaMenuItem.addActionListener(menuItemListener);
      newProvMenuItem.addActionListener(menuItemListener);
      newUserMenuItem.addActionListener(menuItemListener);
      newSalirMenuItem.addActionListener(menuItemListener);
      
      //add menu items to menus
      fileMenu.add(newMenuItem);
      fileMenu.add(openMenuItem);
      fileMenu.add(listaMenuItem);
      fileMenu1.add(newProvMenuItem);
      fileMenu2.add(newUserMenuItem);
      fileMenu3.add(newSalirMenuItem);
   

      //add menu to menubar
      menuBar.add(fileMenu);
      menuBar.add(fileMenu1);
      menuBar.add(fileMenu2);
      menuBar.add(fileMenu3);
     

      //add menubar to the frame
      mainFrame.setJMenuBar(menuBar);
      mainFrame.setVisible(true);     
   }
   
   /**
    * Clase con configuracion de acciones de items
    */
   class MenuItemListener implements ActionListener {
      public void actionPerformed(ActionEvent e) {            
         if(e.getActionCommand().equals("NuevoCliente")){
            VentanaRegistroCliente vregistro = new VentanaRegistroCliente(mainFrame, true); // creamos una ventana
            vregistro.setVisible(true);           // hacemos visible la ventana creada                     
         }else if(e.getActionCommand().equals("EditarCliente")){
            VentanaEditarCliente veditar = new VentanaEditarCliente();      // creamos una ventana
            veditar.setVisible(true);             // hacemos visible la ventana creada      
         }else if(e.getActionCommand().equals("ListarClientes")){
            VentanaListaCliente vlista = new VentanaListaCliente();         // creamos una ventana
            vlista.setVisible(true);              // hacemos visible la ventana creada 
         }else if(e.getActionCommand().equals("NuevoUsuario")){
            VentanaRegistrarUsuario vregUser = new VentanaRegistrarUsuario(mainFrame, true);         // creamos una ventana
            vregUser.setVisible(true);              // hacemos visible la ventana creada 
         }else if(e.getActionCommand().equals("NuevoProveedor")){
            VentanaProveedor vregProv = new VentanaProveedor(mainFrame);         // creamos una ventana
            vregProv.setVisible(true);              // hacemos visible la ventana creada 
         }else if(e.getActionCommand().equals("Salir")){
             DataConnection.delInstance();
             System.exit(0);
         }
      }  
      
      
      
   }
   
    
   
   
}
