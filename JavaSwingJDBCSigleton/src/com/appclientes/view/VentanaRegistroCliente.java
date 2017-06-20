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
import java.util.Properties;
import javax.swing.AbstractButton;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import org.jdatepicker.impl.*;

/**
 * Clase JDialog con el formulario de registro de un cliente
 * @author scabrera
 */
public class VentanaRegistroCliente extends JDialog{
    
    private JLabel lblNit;              // etiqueta para el Nit  
    private JLabel lblRazonSocial;      // etiqueta para la Razon Social    
    private JLabel lblEmail;            // etiqueta para Email   
    private JLabel lblPais;             // etiqueta para Pais    
    private JLabel lblFechaRegistro;    // etiqueta para Fecha de Registro
    private JLabel lblIdioma;           // etiqueta para Idioma   
    private JLabel lblCategoria;        // etiqueta para Categoria
    
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
    private JRadioButton radioCategoria1,radioCategoria2,radioCategoria3;  //Radio button para categoria de cliente   
    private ButtonGroup bgCategorias;   // Botton group para contener radio buttons
    private JButton btnGuardar;              // Botton de guardar
    
    
    
    public VentanaRegistroCliente(JFrame parent, boolean modal){
        super(parent, modal);                    // usamos el contructor de la clase padre JFrame
        configurarVentana(parent , modal);        // configuramos la ventana
        inicializarComponentes();   // inicializamos los atributos o componentes        
    }
        
        
    private void configurarVentana(JFrame parent, boolean modal) {
        this.setTitle("Registro de Clientes");                  // colocamos titulo a la ventana
        this.setSize(450, 550);                                 // colocamos tamanio a la ventana (ancho, alto)
        this.setLocationRelativeTo(parent);                       // centramos la ventana en la pantalla
        this.setLayout(null);                                   // no usamos ningun layout, solo asi podremos dar posiciones a los componentes
        this.setResizable(false);                               // hacemos que la ventana no sea redimiensionable
        this.setModal(modal);
        
    }

    private void inicializarComponentes() {
        // creamos los componentes
        lblNit = new JLabel();
        lblEmail = new JLabel();
        lblPais = new JLabel();
        lblFechaRegistro = new JLabel();
        lblRazonSocial = new JLabel();
        lblIdioma = new JLabel();
        lblCategoria = new JLabel();        
        
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
                    
        
        // configuramos los componentes
        
        // colocamos las etiquetas
        lblNit.setText("Nit:");            
        lblEmail.setText("Email:");        
        lblPais.setText("Pais:");       
        lblFechaRegistro.setText("Fecha de Registro:");    
        lblRazonSocial.setText("Razon Social:");       
        lblIdioma.setText("Idiomas:");     
        lblCategoria.setText("Clasificacion:");        
        btnGuardar.setText("Guardar");      
        
        
        //colocamos posicion y tamanio al componente (x, y, ancho, alto)        
        lblNit.setBounds(50, 50, 150, 25);
        lblEmail.setBounds(50, 100, 150, 25);
        lblPais.setBounds(50, 150, 150, 25);
        lblFechaRegistro.setBounds(50, 200, 200, 25);
        lblRazonSocial.setBounds(50, 250, 200, 25);
        lblIdioma.setBounds(50, 310, 150, 25);
        lblCategoria.setBounds(50, 390, 150, 25);
                
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
        
        btnGuardar.setBounds(100, 470, 200, 30);
               
        
        // adicionamos los componentes a la ventana
        this.add(lblNit);
        this.add(lblEmail);
        this.add(lblPais);  
        this.add(lblFechaRegistro);   
        this.add(lblRazonSocial);   
        this.add(lblIdioma);
        this.add(lblCategoria);
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
        
        Escuchador escucha = new Escuchador();
        btnGuardar.setActionCommand("EventoGuardar");
        btnGuardar.addActionListener(escucha);        
        
    }
    
    
    
    public class Escuchador implements ActionListener{

        @Override
        public void actionPerformed(ActionEvent e) {
            String actionCommand = e.getActionCommand();
            if(actionCommand.equals("EventoGuardar")){
                crearCliente();
            }
        }
    
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
     * Metodo para crear un cliente
     */
    public void crearCliente() {     
        
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
        
        Cliente cliente1 = new Cliente(nit, email, pais, fechaRegistro, 
                razonSocial, idioma, categoria);
        int confirm = JOptionPane.showConfirmDialog(this,
            "¿Confirma que desea guardar este registro?",
            "Confirmar",
            JOptionPane.YES_NO_OPTION);
        if(confirm==0){    
            if(!cliente1.getNit().equals("") && !cliente1.getEmail().equals("")
                && !cliente1.getPais().equals("") && !cliente1.getRazonSocial().equals("")){                                    

                boolean result = DataConnection.getInstance().crearCliente(cliente1);

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
     * Metodo para obtener el valor seleccionado de un grupo de radio button
     * @param buttonGroup
     * @return 
     */
    public String getSelectedButtonText(ButtonGroup buttonGroup) {
        for (Enumeration<AbstractButton> buttons = buttonGroup.getElements(); 
                buttons.hasMoreElements();) {
            AbstractButton button = buttons.nextElement();

            if (button.isSelected()) {
                return button.getText();
            }
        }

        return "Not Selected";
    }    
    

    
}
