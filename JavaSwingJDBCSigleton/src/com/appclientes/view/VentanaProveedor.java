/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.appclientes.view;


import com.appclientes.data.DataConnection;
import com.appclientes.model.Proveedor;
import com.appclientes.model.Sucursal;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;
/**
 *
 * @author scabrera
 */
public class VentanaProveedor extends JDialog{
    
    private JTabbedPane panelPrincipal;
    
    public JPanel panelProveedor;
    
    private JPanel panelSucursales;

    private JLabel lblNit;
    private JLabel lblNombre;
    private JTextField txtNit;
    private JTextField txtNombre;      
    private JLabel lblProveedor;
    private JComboBox cmbProveedor=new JComboBox();
    private JLabel lblSucursal;
    private JTextField txtSucursal;
    private ArrayList<Proveedor> listaProveedores;
    DefaultTableModel modelProveedor;         
    JTable tablaProveedor;    
    private ArrayList<Sucursal> listaSucursales;
    DefaultTableModel modelSucursal;         
    JTable tablaSucursal;    
        
    
    public VentanaProveedor(JFrame menu){
        super();                    // usamos el contructor de la clase padre JFrame
        configVentana(menu);        // configuramos la ventana
        iniComponentes();   // inicializamos los atributos o componentes          
    }
    
    
    private void configVentana(JFrame menu){
        this.setTitle("Proveedores");         // colocamos titulo a la ventana
        this.setSize(600, 500);               // colocamos tamanio a la ventana (ancho, alto)
        this.setLocationRelativeTo(menu);     // centramos la ventana en la pantalla
        this.setLayout(new BorderLayout());   // no usamos ningun layout, solo asi podremos dar posiciones a los componentes
        this.setResizable(false);             // hacemos que la ventana no sea redimiensionable
        this.setModal(true);            
    }
    
    
    private void iniComponentes(){
        panelPrincipal = new JTabbedPane();
        
        panelProveedor = new JPanel();
        configPanelProveedor();
        panelPrincipal.addTab("Proveedor", panelProveedor);
                
        panelSucursales = new JPanel();
        configPanelSucursal();
        panelPrincipal.addTab("Sucursales", panelSucursales);
        
        this.add(panelPrincipal);     
    
    }
    
    
    private void configPanelProveedor(){
        lblNit = new JLabel("Nit: ");
        lblNombre = new JLabel("Nombre: ");
        txtNit = new JTextField();
        txtNombre = new JTextField();
        JButton btnGuardarProveedor = new JButton("Registrar");            
        
        JPanel panelTablaProveedor = new JPanel();        
        JScrollPane scrollPanelProveedor = new JScrollPane();
     
        
        lblNit.setBounds(20, 50, 100, 25);
        txtNit.setBounds(150, 50, 100, 25);
        lblNombre.setBounds(20, 100, 100, 25);
        txtNombre.setBounds(150, 100, 100, 25);     
        btnGuardarProveedor.setBounds(80, 150, 100, 25);
        panelProveedor.setLayout(null);
        panelProveedor.add(lblNit);
        panelProveedor.add(txtNit);
        panelProveedor.add(lblNombre);
        panelProveedor.add(txtNombre);
        panelProveedor.add(btnGuardarProveedor);
        

        panelTablaProveedor.setLayout(new GridLayout());
        panelTablaProveedor.setBounds(300,  50, 250, 300);
        panelTablaProveedor.setBorder(BorderFactory.
                createDashedBorder(Color.blue));        

        // Defining        Model for table 
        modelProveedor = new DefaultTableModel();
        tablaProveedor = new JTable(modelProveedor);
        tablaProveedor.setEnabled(false);

        // Defining Column Names on model 
        modelProveedor.addColumn("Nit");
        modelProveedor.addColumn("Nombre");
        
        // Enable Scrolling on table
        scrollPanelProveedor = 
                new JScrollPane(tablaProveedor,
                JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
                JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        panelTablaProveedor.add(scrollPanelProveedor);
        panelTablaProveedor.setEnabled(false);   
        tablaProveedor.getColumnModel().
                getColumn(0).setPreferredWidth(70);
        tablaProveedor.getColumnModel().
                getColumn(1).setPreferredWidth(250);
        panelProveedor.add(panelTablaProveedor);
        
        actualizarTablaProveedores();
        
        btnGuardarProveedor.addActionListener(new ActionListener(){
            @Override
            public void actionPerformed(ActionEvent e) {
                Proveedor prov = new Proveedor(txtNit.getText(),txtNombre.getText());
                crearProveedor(prov);
            }
        });
        
        
    }
    
    
    
    private void actualizarTablaProveedores(){
        listaProveedores  = DataConnection.getInstance().listarProveedores();
        if(listaProveedores!=null){
            cmbProveedor.removeAllItems();
            deleteAllRows(modelProveedor);
            for(int i=0; i < listaProveedores.size();i++){
                Proveedor p=listaProveedores.get(i);
                if(p!=null){
                    Object[] fila = new Object[5];
                    fila[0] = p.getNit();
                    fila[1] = p.getNombre();
                    modelProveedor.addRow(fila);
                    cmbProveedor.addItem(p);
                }
            }            
        }
        txtNit.setText("");
        txtNombre.setText("");
        tablaProveedor.updateUI();//Actualiza la tabla
    }
    
    private void crearProveedor(Proveedor proveedor){
        if(proveedor.getNit().equals("") || proveedor.getNombre().equals("")){
            JOptionPane.showMessageDialog(null, "Ingrese información completa.");  
        }else{           
            boolean result = DataConnection.getInstance().crearProveedor(proveedor);
            if(result){                
                JOptionPane.showMessageDialog(this, "Guardado exitoso.",
                        "Exitoso!",JOptionPane.INFORMATION_MESSAGE);    // mostramos un mensaje (frame, mensaje)
                actualizarTablaProveedores();                
            }else{
                JOptionPane.showMessageDialog(this, "Error al realizar guardado.",
                        "Error",JOptionPane.ERROR_MESSAGE);    // mostramos un mensaje (frame, mensaje)                        
            }             
            
        }
    }
    
    
    private void configPanelSucursal(){
        lblProveedor = new JLabel("Proveedor: ");
        actualizarTablaProveedores();
        lblSucursal = new JLabel("Sucursal: ");
        txtSucursal = new JTextField();
        JButton btnGuardarSucursal = new JButton("Registrar");            
        
        JPanel panelTablaSucursal = new JPanel();
        JScrollPane scrollPanelSucursal = new JScrollPane();
     
        
        lblProveedor.setBounds(20, 50, 100, 25);
        cmbProveedor.setBounds(150, 50, 100, 25);
        lblSucursal.setBounds(20, 100, 100, 25);
        txtSucursal.setBounds(150, 100, 100, 25);       
        btnGuardarSucursal.setBounds(80, 200, 100, 25);
        
        panelSucursales.setLayout(null);
        panelSucursales.add(lblProveedor);
        panelSucursales.add(cmbProveedor);
        panelSucursales.add(lblSucursal);
        panelSucursales.add(txtSucursal);        
        panelSucursales.add(btnGuardarSucursal);                

        panelTablaSucursal.setLayout(new GridLayout());
        panelTablaSucursal.setBounds(280, 50, 250, 350);
        panelTablaSucursal.setBorder(BorderFactory.createDashedBorder(Color.blue));        

        // Defining        Model for table 
        modelSucursal = new DefaultTableModel();
        tablaSucursal = new JTable(modelSucursal);
        tablaSucursal.setEnabled(false);

        // Defining Column Names on model 
        modelSucursal.addColumn("Proveedor");
        modelSucursal.addColumn("Sucursal");

        // Enable Scrolling on table
        scrollPanelSucursal = new JScrollPane(tablaSucursal, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
                JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        panelTablaSucursal.add(scrollPanelSucursal);
        panelTablaSucursal.setEnabled(false);   
        tablaSucursal.getColumnModel().getColumn(0).setPreferredWidth(70);
        tablaSucursal.getColumnModel().getColumn(1).setPreferredWidth(150);

        panelSucursales.add(panelTablaSucursal);
        actualizarTablaSucursales();
        btnGuardarSucursal.addActionListener(new ActionListener(){
            @Override
            public void actionPerformed(ActionEvent e) {
                Proveedor proveedorSelec = (Proveedor)cmbProveedor.getSelectedItem();
                if(proveedorSelec!=null){
                    Sucursal sucursal = new Sucursal(proveedorSelec,txtSucursal.getText());
                    crearSucursal(sucursal);
                }
            }
        });
        
        
        
        
    }    
    
    private void crearSucursal(Sucursal sucursal){
        if(sucursal.getProveedor().getNit().equals("") || sucursal.getSucursal().equals("")){
            JOptionPane.showMessageDialog(null, "Ingrese información completa.");
        }else{
            boolean result = DataConnection.getInstance().crearSucursal(sucursal);
            if(result){
                JOptionPane.showMessageDialog(this, "Guardado exitoso.",
                        "Exitoso!",JOptionPane.INFORMATION_MESSAGE);    // mostramos un mensaje (frame, mensaje)
                actualizarTablaSucursales();
            }else{
                JOptionPane.showMessageDialog(this, "Error al realizar guardado.",
                        "Error",JOptionPane.ERROR_MESSAGE);    // mostramos un mensaje (frame, mensaje)                        
            }              
        }
    }     
    
    private void actualizarTablaSucursales(){
        listaSucursales  = DataConnection.getInstance().listarSucursales();
        if(listaSucursales!=null){
            deleteAllRows(modelSucursal);
            for(int i=0; i < listaSucursales.size();i++){
                Sucursal s=listaSucursales.get(i);
                if(s!=null){
                    Object[] fila = new Object[5];
                    fila[0] = s.getProveedor().getNit();
                    fila[1] = s.getSucursal();
                    modelSucursal.addRow(fila);
                }
            }            
        }

        tablaSucursal.updateUI();//Actualiza la tabla
    }
    
    
    
    public static void deleteAllRows(
            final DefaultTableModel model) {
        for( int i = model.getRowCount() - 1; 
                i >= 0; i-- ) {
            model.removeRow(i);
        }
    }


}
