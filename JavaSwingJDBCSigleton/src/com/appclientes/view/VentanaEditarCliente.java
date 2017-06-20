/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.appclientes.view;



import com.appclientes.data.DataConnection;
import com.appclientes.model.Cliente;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.swing.AbstractButton;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import org.jdatepicker.impl.*;

/**
 * Clase JDialog con la ventana de edicion o eliminacion de cliente
 * @author scabrera
 */
public class VentanaEditarCliente extends JDialog{
    
    private JLabel lblCodigo;           // etiqueta para el codigo  
    private JLabel lblNit;              // etiqueta para el Nit  
    private JLabel lblRazonSocial;      // etiqueta para la Razon Social    
    private JLabel lblEmail;            // etiqueta para Email   
    private JLabel lblPais;             // etiqueta para Pais    
    private JLabel lblFechaRegistro;    // etiqueta para Fecha de Registro
    private JLabel lblIdioma;           // etiqueta para Idioma   
    private JLabel lblCategoria;        // etiqueta para Categoria
    
    private JTextField txtCodigo;       // caja de texto para Codigo de cliente
    private JTextField txtNit;          // caja de texto para Nit
    private JTextField txtEmail;        // caja de texto para Email
    private final String[] paisStrings = { "Colombia", "Chile", "Argentina", "Uruguay", "Peru" };
    private JComboBox paisList;         // combo para lista de Paises
    private JDatePickerImpl datePicker; // calendario para Fecha de Registro
    private JScrollPane scrollpane1;    // Scroll de panel de JTextArea de Razon Social
    private JTextArea txtARazonSocial;  // text area para Razon Social
    private JCheckBox checkIdioma1;     // Check para idioma1
    private JCheckBox checkIdioma2;     // Check para idioma2
    private JCheckBox checkIdioma3;     // Check para idioma3
    private JRadioButton radioCategoria1,radioCategoria2,radioCategoria3;  //Radio button para categoria 
    private ButtonGroup bgCategorias;   // Botton group para contener radio buttons
    private JButton btnGuardar;         // Botton de guardar
    private JButton btnEliminar;        // Botton de eliminar

    
    
    
    public VentanaEditarCliente(){
        super();                    // usamos el contructor de la clase padre JDialog
        configurarVentana();        // configuramos la ventana
        inicializarComponentes();   // inicializamos los atributos o componentes        
    }
        
        
    private void configurarVentana() {
        this.setTitle("Editar Clientes");   // colocamos titulo a la ventana
        this.setSize(450, 550);                  // colocamos tamanio a la ventana (ancho, alto)
        this.setLocationRelativeTo(null);        // centramos la ventana en la pantalla
        this.setLayout(null);                    // no usamos ningun layout, solo asi podremos dar posiciones a los componentes
        this.setResizable(false);                // hacemos que la ventana no sea redimiensionable
        this.setModal(true);
        
    }

    private void inicializarComponentes() {
        
        // creamos los componentes
        lblCodigo = new JLabel();
        lblNit = new JLabel();
        lblEmail = new JLabel();
        lblPais = new JLabel();
        lblFechaRegistro = new JLabel();
        lblRazonSocial = new JLabel();
        lblIdioma = new JLabel();
        lblCategoria = new JLabel();        

        txtCodigo = new JTextField();
        txtNit = new JTextField();
        txtEmail = new JTextField(); 

        paisList = new JComboBox(paisStrings);

        datePicker = createJDatePicker(); 

        txtARazonSocial = new JTextArea();
        scrollpane1=new JScrollPane(txtARazonSocial);

        checkIdioma1=new JCheckBox("Inglés");
        checkIdioma2=new JCheckBox("Francés");
        checkIdioma3=new JCheckBox("Alemán");

        bgCategorias=new ButtonGroup();
        radioCategoria1=new JRadioButton("Bajo");
        radioCategoria2=new JRadioButton("Medio");        
        radioCategoria3=new JRadioButton("Alto");                        

        btnGuardar = new JButton();  
        btnEliminar = new JButton();                      
                    
        
        // configuramos los componentes
        
        // colocamos las etiquetas
        lblCodigo.setText("Código:");  
        lblNit.setText("Nit:");            
        lblEmail.setText("Email:");        
        lblPais.setText("Pais:");       
        lblFechaRegistro.setText("Fecha de Registro:");    
        lblRazonSocial.setText("Razon Social:");       
        lblIdioma.setText("Idiomas:");     
        lblCategoria.setText("Clasificacion:");        
        btnGuardar.setText("Guardar");   
        btnEliminar.setText("Eliminar");   
        
        //colocamos posicion y tamanio al componente (x, y, ancho, alto)        
        lblCodigo.setBounds(50, 10, 150, 25);
        lblNit.setBounds(50, 50, 150, 25);
        lblEmail.setBounds(50, 100, 150, 25);
        lblPais.setBounds(50, 150, 150, 25);
        lblFechaRegistro.setBounds(50, 200, 200, 25);
        lblRazonSocial.setBounds(50, 250, 200, 25);
        lblIdioma.setBounds(50, 310, 150, 25);
        lblCategoria.setBounds(50, 390, 150, 25);
                
        txtCodigo.setBounds(200, 10, 100, 25);
        txtNit.setBounds(200, 50, 100, 25);
        txtEmail.setBounds(200, 100, 200, 25);
        paisList.setBounds(200, 150, 100, 25);
        datePicker.setBounds(200, 200, 150, 25);
        scrollpane1.setBounds(200, 250, 200, 60);
        checkIdioma1.setBounds(200,310,150,30);
        checkIdioma2.setBounds(200,335,150,30);
        checkIdioma3.setBounds(200,360,150,30);             

        radioCategoria1.setBounds(200,390,100,30);
        radioCategoria2.setBounds(200,415,100,30);
        radioCategoria3.setBounds(200,440,100,30);                
        
        btnGuardar.setBounds(100, 470, 100, 30);
        btnEliminar.setBounds(210, 470, 100, 30);
        
        // desabilitamos los botones guardar y eliminar, se habilitan al seleccionar un cliente
        btnGuardar.setEnabled(false);
        btnEliminar.setEnabled(false);       
        
        // adicionamos los componentes a la ventana
        this.add(lblCodigo);
        this.add(lblNit);
        this.add(lblEmail);
        this.add(lblPais);  
        this.add(lblFechaRegistro);   
        this.add(lblRazonSocial);   
        this.add(lblIdioma);
        this.add(lblCategoria);
        this.add(txtCodigo);
        this.add(txtNit);
        this.add(txtEmail);
        this.add(paisList);       
        this.add(datePicker);
        this.add(scrollpane1);
        this.add(checkIdioma1);                  
        this.add(checkIdioma2);                
        this.add(checkIdioma3);  
        this.add(radioCategoria1);
        bgCategorias.add(radioCategoria1);        
        this.add(radioCategoria2);
        bgCategorias.add(radioCategoria2);        
        this.add(radioCategoria3);
        bgCategorias.add(radioCategoria3);          
        this.add(btnGuardar);
        this.add(btnEliminar);
        
        
        
        // hacemos que el boton guardar tenga una accion         
        Escuchador escucha = new Escuchador();
        btnGuardar.setActionCommand("EventoEditar");
        btnGuardar.addActionListener(escucha);  
        
        // hacemos que el boton eliminar tenga una accion               
        btnEliminar.setActionCommand("EventoEliminar");
        btnEliminar.addActionListener(escucha);  
        
        // hacemos que el evento enter de la caja de texto codigo tenga una accion       
        txtCodigo.setActionCommand("EventoConsultar");
        txtCodigo.addActionListener(escucha);         
               
        
    }
    
public class Escuchador implements ActionListener{

        @Override
        public void actionPerformed(ActionEvent e) {
            String actionCommand = e.getActionCommand();
            switch (actionCommand) {
                case "EventoEditar":
                    {
                        int codigo = Integer.parseInt(txtCodigo.getText());
                        editarCliente(codigo);
                        break;
                    }
                case "EventoEliminar":
                    {
                        int codigo = Integer.parseInt(txtCodigo.getText());
                        eliminarCliente(codigo);
                        break;
                    }            
                case "EventoConsultar":
                    {
                        int codigo = Integer.parseInt(txtCodigo.getText());
                        boolean result = mostrarCliente(codigo);
                        //se habilita o inhabilita boton guardar y caja de texto codigo dependiendo de la busqueda
                        btnGuardar.setEnabled(result);
                        btnEliminar.setEnabled(result);
                        txtCodigo.setEnabled(!result);
                        break;
                    }
                default:
                    break;
            }
        }
    
    }
        
    
    /**
     * metodo para buscar un cliente por el codigo
     * @param codigo
     * @return 
     */        
    public Cliente buscarCliente(int codigo){        
	Cliente cliente = DataConnection.getInstance().buscarCliente(codigo);
        return cliente;
    }     
    
    /**
     * Metodo para mostrar los datos del cliente en pantalla
     * @param codigo
     * @return true si encontro un cliente false si no encontro un cliente
     */
    public boolean mostrarCliente(int codigo){
        Cliente cliente =buscarCliente(codigo);
        boolean result = false;
        if(cliente!=null){
            txtNit.setText(cliente.getNit());	     
            txtEmail.setText(cliente.getEmail());	     
            paisList.setSelectedItem(cliente.getPais());	     
            Calendar cal = new GregorianCalendar();
            cal.setTime(cliente.getFechaRegistro());
            datePicker.getModel().setDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH));               
            txtARazonSocial.setText(cliente.getRazonSocial());	                 
            if(cliente.getIdioma().contains("Inglés")){
                checkIdioma1.setSelected(true);
            }
            if(cliente.getIdioma().contains("Francés")){
                checkIdioma2.setSelected(true);
            }
            if(cliente.getIdioma().contains("Alemán")){
                checkIdioma3.setSelected(true);
            }                
            if(cliente.getCategoria().equals("Bajo")){
                radioCategoria1.setSelected(true);
            }else if(cliente.getCategoria().equals("Medio")){
                radioCategoria2.setSelected(true);
            }else if(cliente.getCategoria().equals("Alto")){
                radioCategoria3.setSelected(true);
            }  	     
            
            result = true;
        }      
        return result;
    }
    
    /**
     * metodo para crear componente de fecha
     * @return 
     */
    public JDatePickerImpl createJDatePicker(){
        UtilDateModel model = new UtilDateModel();
        Calendar fecha = GregorianCalendar.getInstance();
        int year = fecha.get(Calendar.YEAR);
        int month = fecha.get(Calendar.MONTH);
        int day = fecha.get(Calendar.DAY_OF_MONTH);
        
        model.setDate(year,month,day);
        model.setSelected(true);
        Properties p = new Properties();
        p.put("text.today", "Today");
        p.put("text.month", "Month");
        p.put("text.year", "Year");
        JDatePanelImpl datePanel = new JDatePanelImpl(model, p);
        JDatePickerImpl jdatePicker = new JDatePickerImpl(datePanel, new DateLabelFormatter());    
        return jdatePicker;
    }

    
    /**
     * Metodo para editar un cliente
     */
    public void editarCliente(int codigo) {     
        
        //obtenemos los valores de los componentes
        String nit= txtNit.getText();
        String email=txtEmail.getText();
        String pais=(String)paisList.getSelectedItem();
        Date fechaRegistro = (Date) datePicker.getModel().getValue();
        String razonSocial= txtARazonSocial.getText();
        String idioma="Español";
        if(checkIdioma1.isSelected()){
            idioma = idioma + ", Inglés";
        }
        if(checkIdioma2.isSelected()){
            idioma = idioma + ", Francés";
        }
        if(checkIdioma3.isSelected()){
            idioma = idioma + ", Alemán";
        }                                        
        String categoria=getSelectedButtonText(bgCategorias);
        Cliente cliente1 = new Cliente(codigo,nit, email, pais, fechaRegistro, 
                razonSocial, idioma, categoria);                
        int confirm = JOptionPane.showConfirmDialog(this,
            "¿Confirma que desea guardar este registro?",
            "Confirmar",
            JOptionPane.YES_NO_OPTION);
        if(confirm==0){    
            if(codigo!=0 && !nit.equals("") && !email.equals("")
                && !pais.equals("") && !razonSocial.equals("")){                                    

                boolean result = DataConnection.getInstance().editarCliente(cliente1);

                if(result){
                    dispose();
                    JOptionPane.showMessageDialog(this, "Guardado exitoso de cliente.",
                            "Exitoso!",JOptionPane.INFORMATION_MESSAGE);    // mostramos un mensaje (frame, mensaje)
                }else{
                    JOptionPane.showMessageDialog(this, "Error al realizar guardado de cliente."
                            + "{"+nit+","+email+","+pais+","+fechaRegistro+","+razonSocial+""
                            + ","+idioma+","+categoria+"}",
                            "Error",JOptionPane.ERROR_MESSAGE);    // mostramos un mensaje (frame, mensaje)                        
                }
            }else{
                JOptionPane.showMessageDialog(this, "Debe ingresar la información completa..",
                        "Información!",JOptionPane.INFORMATION_MESSAGE);    // mostramos un mensaje (frame, mensaje)                
            }
        }           
    }   
    
    /**
     * Metodo para eliminar un cliente
     * @param codigo 
     */
    public void eliminarCliente(int codigo) {                    
        int confirm = JOptionPane.showConfirmDialog(this,
            "¿Confirma que desea eliminar este registro?",
            "Confirmar",
            JOptionPane.YES_NO_OPTION);
        if(confirm==0){    
            if(codigo!=0){                                    
                boolean result = DataConnection.getInstance().eliminarCliente(codigo);
                if(result){
                    dispose();
                    JOptionPane.showMessageDialog(this, "Eliminado exitoso de cliente.",
                            "Exitoso!",JOptionPane.INFORMATION_MESSAGE);    // mostramos un mensaje (frame, mensaje)
                }else{
                    JOptionPane.showMessageDialog(this, "Error al realizar eliminacion de cliente."
                            + "{"+codigo+"}",
                            "Error",JOptionPane.ERROR_MESSAGE);    // mostramos un mensaje (frame, mensaje)                        
                }
            }else{
                JOptionPane.showMessageDialog(this, "Codigo de cliente no especificado.",
                        "Información!",JOptionPane.INFORMATION_MESSAGE);    // mostramos un mensaje (frame, mensaje)                
            }
        }           
    }   
      
    
    
    /**
     * Metodo para obtener el valor seleccionado de un grupo de radio button
     * @param buttonGroup
     * @return 
     */
    public String getSelectedButtonText(ButtonGroup buttonGroup) {
        for (Enumeration<AbstractButton> buttons = buttonGroup.getElements(); buttons.hasMoreElements();) {
            AbstractButton button = buttons.nextElement();

            if (button.isSelected()) {
                return button.getText();
            }
        }

        return "Not Selected";
    }    
    

    
}
