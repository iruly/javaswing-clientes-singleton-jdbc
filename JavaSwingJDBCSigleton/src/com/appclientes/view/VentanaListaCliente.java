package com.appclientes.view;

import com.appclientes.data.DataConnection;
import com.appclientes.model.Cliente;
import java.awt.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import javax.swing.JDialog;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

public class VentanaListaCliente extends JDialog {

    private JTable table;
    private DefaultTableModel modelo;    
    
    public VentanaListaCliente() {
        super();
        configurarVentana();
        inicializarComponentes();
    }    

    private void configurarVentana() {
        this.setTitle("Lista de Clientes");     // colocamos titulo a la ventana
        this.setSize(450, 550);                 // colocamos tamanio a la ventana (ancho, alto)
        this.setResizable(false);               // hacemos que la ventana no sea redimiensionable
        this.setModal(true);                    // bloqueo de ventanas de fondo
        this.setLocationRelativeTo(null);       // centramos la ventana en la pantalla
    }
    
    
    private void inicializarComponentes() {
               
        modelo = new DefaultTableModel() {
           @Override
           public boolean isCellEditable(int fila, int columna) {
               return false; //Con esto conseguimos que la tabla no se pueda editar
           }
        };

        table = new JTable(modelo); //Metemos el modelo dentro de la tabla        
        modelo.addColumn("Código"); //Añadimos las columnas a la tabla (tantas como queramos)
        modelo.addColumn("Nit");        
        modelo.addColumn("Razon Social");        
        modelo.addColumn("Fecha de Registro");        
        modelo.addColumn("Categoría");        
              
        /* consultamos los clientes registrados y los cargamos en el modelo*/
        ArrayList<Cliente> listaClientes  = buscarClientes();
        for(int i=0; i < listaClientes.size();i++){
            Cliente cl=listaClientes.get(i);
            if(cl!=null){
                Object[] fila = new Object[5];
                fila[0] = cl.getCodigo();
                fila[1] = cl.getNit();
                fila[2] = cl.getRazonSocial();
                fila[3] = formatearFecha(cl.getFechaRegistro());
                fila[4] = cl.getCategoria();
                modelo.addRow(fila);
            }
        }
        table.updateUI();//Actualiza la tabla



        //Create the scroll pane and add the table to it. 
        JScrollPane scrollPane = new JScrollPane(table);

        //Add the scroll pane to this window.
        getContentPane().add(scrollPane, BorderLayout.CENTER);        
    }    
    
    /**
     * metodo de busqueda de clientes registrados en la base de datos
     * @return lista de clientes
     */
    public ArrayList<Cliente> buscarClientes() {     
       return DataConnection.getInstance().listarClientes();            
    }       
    
    /**1
     * metodo para formatear la fecha que viene de la base de datos
     * @param fecha en formato Mon Feb 13 00:00:00 COT 2017
     * @return String en formato 13/02/2017
     */
    public static String formatearFecha(Date fecha) {
        SimpleDateFormat dateFormat  = new SimpleDateFormat("dd/MM/yyyy");
        String fechaFormateada;
        if(fecha!=null){
            fechaFormateada = dateFormat.format(fecha);
        }else{
            fechaFormateada="";
        }	    
        return fechaFormateada;
    }   

}