package com.appclientes.data;

import com.appclientes.model.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Clase utileria para realizar conexion con base de datos
 * @author scabrera
 */
public class DataConnection {

    static Connection con; // atributo para guardar el objeto conexion
    private static DataConnection INSTANCE = null;
 
    /**
     * Metodo constructor que ejecuta el metodo para conectar con la base de dades
     */
    private DataConnection() {
    	performConnection();
    }
 
 
    /**
     * Crea una instancia de conexion de acceso a la base de datos si no esta creada
     */
    private synchronized static void createInstance() {
        if (INSTANCE == null) { 
            INSTANCE = new DataConnection();
        }
    }
 
    /**Metodo para retorna una instancia de la conexion. Si no esta creada la crea, y si esta creada la retorna
     * @return retorna una instancia de la conexion a la base de datos
     */
    public static DataConnection getInstance() {
        if (INSTANCE == null) createInstance();
        return INSTANCE;
    }
    
    /**
     * Metodo para cerrar conexion a base de datos
     */
    public static void delInstance() {
    	INSTANCE = null;
    	closeConnection();
    }


        /**
         * Metodo para realizar la conexion a la base de datos
         */
	/**public void performConnection() {

		String host = "127.0.0.1";
		String user = "estudiant";
		String pass = "estudiant";
                String dtbs = "LeagueManager";
		String dtbs = "estudiant";

		try { 
                    Class.forName("com.mysql.jdbc.Driver");
                    String newConnectionURL = "jdbc:mysql://" + host + "/" + dtbs + "?"
                                    + "user=" + user + "&password=" + pass;
                    con = DriverManager.getConnection(newConnectionURL);
                    con.setAutoCommit(false);
		} catch (Exception e) {
			System.out.println("Error al abrir conexion.");
		}
	}*/
    
    
        /**
         * Metodo para realizar la conexion a la base de datos MS SQL Server
         */
        public void performConnection() {
		String user = "dbadmin";
		String pass = "adsi2017";

		try {
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			String newConnectionURL = "jdbc:jtds:sqlserver://localhost/dbclientes";
                        
			con = DriverManager.getConnection(newConnectionURL,user, pass);
			con.setAutoCommit(false);
		}catch (ClassNotFoundException e) {
			System.out.println("Controlador Mysql no encontrado.");
                        e.printStackTrace();
		}catch (SQLException e) {
			System.out.println("Error SQL al abrir conexion.");
                        e.printStackTrace();
		}catch (Exception e) {
			System.out.println("Error al abrir conexion.");
                        e.printStackTrace();
		}
	}    

	/**
	 * Metodo para cerrar conexion de base de datos
	 */
	public static void closeConnection() {
		try {
			con.close();
		} catch (Exception e) {
			System.out.println("Error al cerrar la conexion.");
		}
	}
	
	
        /** Ejecucion de sentencias de manipulacion de datos */
        
	/**
         * Metodo para consultar clientes
         * @return lista de clientes
         */
	public ArrayList<Cliente> listarClientes(){
            ArrayList<Cliente> ls = new ArrayList<Cliente>();
            try{
		String seleccion = "SELECT codigo, nit, email, pais, fecharegistro, razonsocial, idioma, categoria FROM clientes";
		PreparedStatement ps = con.prepareStatement(seleccion);
                //para marca un punto de referencia en la transacción para hacer un ROLLBACK parcial.
		ResultSet rs = ps.executeQuery();
		con.commit();
		while(rs.next()){
                        Cliente cl = new Cliente();
                        cl.setCodigo(rs.getInt("codigo"));
                        cl.setNit(rs.getString("nit"));
                        cl.setRazonSocial(rs.getString("razonsocial"));
                        cl.setCategoria(rs.getString("categoria"));
                        cl.setEmail(rs.getString("email"));
                        Calendar cal = new GregorianCalendar();
                        cal.setTime(rs.getDate("fecharegistro"));                        
                        cl.setFechaRegistro(cal.getTime());
                        cl.setIdioma(rs.getString("idioma"));
                        cl.setPais(rs.getString("pais"));
			ls.add(cl);
		}
            }catch(Exception e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }             
            return ls;
	}
	
        
       
        /**
         * Metodo para consultar un cliente por el codigo en la base de datos
         * @param codigo
         * @return mapa con atributos de cliente
         */
	public Cliente buscarCliente(int codigo) {
            Cliente cliente =new Cliente();
            try{
		String seleccion = "SELECT codigo, nit, email, pais, fecharegistro, razonsocial, "
                        + "idioma, categoria FROM clientes where codigo=? ";
		PreparedStatement ps = con.prepareStatement(seleccion);
                ps.setInt(1, codigo);
		Savepoint sp1 = con.setSavepoint("SAVE_POINT_ONE");
		ResultSet rs = ps.executeQuery();
		con.commit();
		while(rs.next()){
                    cliente.setCodigo(rs.getInt("codigo"));
                    cliente.setNit(rs.getString("nit"));
                    cliente.setEmail(rs.getString("email"));
                    cliente.setPais(rs.getString("pais"));
                    cliente.setFechaRegistro(rs.getDate("fecharegistro"));
                    cliente.setRazonSocial(rs.getString("razonsocial"));
                    cliente.setIdioma(rs.getString("idioma"));
                    cliente.setCategoria(rs.getString("categoria"));                    
		}               
            }catch(Exception e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
            return cliente;
	}                                   
        
        
        /**
         * Metodo para crear un cliente
         * @param cliente
         * @return true si la transaccion fue exitosa o false si hubo excepcion
         */
	public boolean crearCliente(Cliente cliente) {
		try{
			String insert = "INSERT INTO clientes(nit, email, pais, fecharegistro, "
                                + "razonsocial, idioma, categoria) VALUES ( ?, ?, ?, ?, ?, ?, ? );";
			PreparedStatement ps = con.prepareStatement(insert);
			ps.setString(1, cliente.getNit());
                        ps.setString(2, cliente.getEmail());
                        ps.setString(3, cliente.getPais());
                        ps.setDate(4, new java.sql.Date(cliente.getFechaRegistro().getTime()));
                        ps.setString(5, cliente.getRazonSocial());
                        ps.setString(6, cliente.getIdioma());
                        ps.setString(7, cliente.getCategoria());
                        //para marca un punto de referencia en la transacción para hacer un ROLLBACK parcial.                        
			Savepoint sp1 = con.setSavepoint("SAVE_POINT_ONE");			
			ps.executeUpdate();	
			con.commit();
			return true;
		}catch(Exception e){
                        System.out.println(e.getMessage());
                        e.printStackTrace();
			return false;
		}
	}
	
        /**
         * Metodo para eliminar cliente
         * @param codigo codigo unico de cliente
         * @return true si la transaccion fue exitosa o false si hubo excepcion
         */
	public boolean eliminarCliente(int codigo){
		try{
			String insert = "DELETE FROM clientes WHERE codigo = ? ;";
			PreparedStatement ps = con.prepareStatement(insert);
			ps.setInt(1, codigo);
			Savepoint sp1 = con.setSavepoint("SAVE_POINT_ONE");			
			ps.executeUpdate();	
			con.commit();
			return true;
		}catch(Exception e){
                        System.out.println(e.getMessage());
                        e.printStackTrace();
			return false;
		}
	}
	
        /**
         * Metodo para editar un cliente
         * @param cliente 
         *              objeto de tipo cliente
         * @return true si la transaccion fue exitosa o false si hubo excepcion
         */
	public boolean editarCliente(Cliente cliente){
		try{
			String insert = "UPDATE clientes SET nit=?, email=?, pais=?, "
                                + "fecharegistro=?, razonsocial=?, idioma=?, categoria=? WHERE codigo = ? ;";
			PreparedStatement ps = con.prepareStatement(insert);
			ps.setString(1, cliente.getNit());
			ps.setString(2, cliente.getEmail());
                        ps.setString(3, cliente.getPais());
                        ps.setDate(4, new java.sql.Date(cliente.getFechaRegistro().getTime()));
                        ps.setString(5, cliente.getRazonSocial());
                        ps.setString(6, cliente.getIdioma());
                        ps.setString(7, cliente.getCategoria());
                        ps.setInt(8, cliente.getCodigo());
			Savepoint sp1 = con.setSavepoint("SAVE_POINT_ONE");
			ps.executeUpdate();	
			con.commit();
			return true;
		}catch(Exception e){
                        System.out.println(e.getMessage());
                        e.printStackTrace();
			return false;
		}
	}
        
        
        
        /**
         * Metodo para creacion de usuario en base de datos
         * @param usuario
         * @return 
         */
        public boolean crearUsuario(Usuario usuario) {
		try{
                    String insert = "INSERT INTO usuarios(usuario,nombre, clave) "
                            + " VALUES (?, ?, ? );";
                    PreparedStatement ps = con.prepareStatement(insert);
                    ps.setString(1, usuario.getUsuario());
                    ps.setString(2, usuario.getNombre());
                    ps.setString(3, usuario.getClave());                      
                    //para marca un punto de referencia en la transacción para hacer un ROLLBACK parcial.                        
                    Savepoint sp1 = con.setSavepoint("SAVE_POINT_ONE");			
                    ps.executeUpdate();	
                    con.commit();
                    return true;
		}catch(Exception e){
                        System.out.println(e.getMessage());
                        e.printStackTrace();
			return false;
		}
	}     
        
        /**
         * Metodo para buscar un usuario por id de usuario
         * @param usuario
         * @return 
         */
        public Usuario buscarUsuario(String usuario) {
            Usuario usuario1 = new Usuario();
            try{
		String seleccion = "SELECT usuario, nombre, clave FROM usuarios where usuario=? ";
		PreparedStatement ps = con.prepareStatement(seleccion);
                ps.setString(1, usuario);
                
		//Savepoint sp1 = con.setSavepoint("SAVE_POINT_ONE");
                
		ResultSet rs = ps.executeQuery();
		con.commit();
		while(rs.next()){
                   usuario1.setUsuario(rs.getString("usuario"));                      
                   usuario1.setNombre(rs.getString("nombre"));                      
                   usuario1.setClave(rs.getString("clave")); 
                   return usuario1;
		}    
                 
            }catch(Exception e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
            return null;        

        }        

        
        

        public boolean crearProveedor(Proveedor proveedor) {
		try{
			String insert = "INSERT INTO proveedores(nit, nombre) VALUES (?, ? );";
			PreparedStatement ps = con.prepareStatement(insert);
			ps.setString(1, proveedor.getNit());
                        ps.setString(2, proveedor.getNombre());             
                        //para marca un punto de referencia en la transacción para hacer un ROLLBACK parcial.                        
			Savepoint sp1 = con.setSavepoint("SAVE_POINT_ONE");			
			ps.executeUpdate();	
			con.commit();
			return true;
		}catch(Exception e){
                        System.out.println(e.getMessage());
                        e.printStackTrace();
			return false;
		}
	}           
        
        public boolean crearSucursal(Sucursal sucursal) {
		try{
			String insert = "INSERT INTO sucursales(nit, sucursal) VALUES (?, ? );";
			PreparedStatement ps = con.prepareStatement(insert);
			ps.setString(1, sucursal.getProveedor().getNit());
                        ps.setString(2, sucursal.getSucursal());             
                        //para marca un punto de referencia en la transacción para hacer un ROLLBACK parcial.                        
			Savepoint sp1 = con.setSavepoint("SAVE_POINT_ONE");			
			ps.executeUpdate();	
			con.commit();
			return true;
		}catch(Exception e){
                        System.out.println(e.getMessage());
                        e.printStackTrace();
			return false;
		}
	}   
        
        
        
	public ArrayList<Proveedor> listarProveedores(){
            ArrayList<Proveedor> ls = new ArrayList();
            try{
		String seleccion = "SELECT nit, nombre FROM proveedores";
		PreparedStatement ps = con.prepareStatement(seleccion);
                //para marca un punto de referencia en la transacción para hacer un ROLLBACK parcial.
		ResultSet rs = ps.executeQuery();
		con.commit();
		while(rs.next()){
                        Proveedor p = new Proveedor();
                        p.setNit(rs.getString("nit"));
                        p.setNombre(rs.getString("nombre"));
                        System.out.println("val"+rs.getString("nombre"));
			ls.add(p);
		}
                return ls;
            }catch(Exception e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }             
            return null;
	}   
        
        
        
	public ArrayList<Sucursal> listarSucursales(){
            ArrayList<Sucursal> ls  = new ArrayList();
            try{
		String seleccion = "SELECT sucursales.nit, sucursales.sucursal, proveedores.nombre FROM sucursales"
                        + " inner join proveedores on sucursales.nit=proveedores.nit ";
		PreparedStatement ps = con.prepareStatement(seleccion);
                //para marca un punto de referencia en la transacción para hacer un ROLLBACK parcial.
		ResultSet rs = ps.executeQuery();
		con.commit();
		while(rs.next()){
                        Sucursal s = new Sucursal();
                        Proveedor p = new Proveedor(rs.getString("nit"),rs.getString("nombre"));
                        s.setProveedor(p);
                        s.setSucursal(rs.getString("sucursal"));                      
			ls.add(s);
		}
                return ls;
            }catch(Exception e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }             
            return null;
	}             
}
