package co.bancolombia.flume.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.Security;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * Clase utilizada para desencriptar constraseñas en el flujo de Flume.  La clase utiliza en algoritmo AES usando el proveedor 
 * BouncyCastle (Se debe añadir como parte del ClassPath).<br><br>
 * Para desencriptar requiere un salt leído de un archivo, y este se codifica en un Hash que sirve como llave de desencripción
 * @author rlarios
 *
 */
public class PwDecryptor {

	/**
	 * Desencripta un password usando un salt enviado como parametro de archivo.
	 * @param keyFilePath ruta del archivo donde se encuentra la llave publica / Archivo de salt
	 * @param encrypted password en forma encriptada anteriormente
	 * @return la contraseña desencriptada
	 * @throws Exception En el caso que haya algun error en el proceso de desencripción
	 */
	public static String decrypt(String keyFilePath, String encrypted) throws Exception{
		String decrypted = null;

		String salt = getSaltFromFile(keyFilePath);

		decrypted = desencriptar("AES/ECB/PKCS7Padding" , encrypted , salt);

		return decrypted;
	}


	/**
	 * Desencripta un mensaje usando un salt y un algoritmo específico.
	 * 
	 * @param algoritmo algortimo de encripción a utilizar
	 * @param mensaje Mensaje a encriptar
	 * @param salt sal para generar la llave privada
	 * @return El mensaje desencriptado
	 * @throws Exception En el caso que haya algun error en el proceso de desencripción
	 */
	private static String desencriptar(String algoritmo , String mensaje, String salt) throws Exception{
		Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());  
		Cipher cipher = Cipher.getInstance(algoritmo, "BC");
		byte[] cipherText = Base64.decodeBase64(mensaje.getBytes("UTF-8"));

		//System.out.println("\nDesEncriptando=" +  mensaje);

		SecretKeySpec key = getKey(salt);
		cipher.init(Cipher.DECRYPT_MODE, key);

		byte[] plainText = cipher.doFinal(cipherText);

		return  new String(plainText,"UTF-8");

	}

	/**
	 * Genera una llave (SecretKeySpec) para el formato AES usando el salt enviado.
	 * @param salt Sal que se usa para generar la clave
	 * @return el SecretKeySpec generado
	 * @throws Exception En el caso que haya algun error en el proceso de generación de llave
	 */
	private static SecretKeySpec getKey( String salt) throws Exception {
		String xk = "PIQmDzXjscTIyzqB" + salt;
		String newKey = DigestUtils.shaHex(xk.getBytes("UTF-8")).substring(0, 16);
		byte[] keyBytes = newKey.getBytes("UTF-8"); 

		return new SecretKeySpec(keyBytes, "AES/ECB/PKCS7Padding");
	}

	/**
	 * Devuelve el Salt almacenado en el archivo.  Lee linea por línea y concatena dichas líneas
	 * @param keyFilePath Ruta del archivo con la sal.  Debe estar en formato UTF-8
	 * @return la sal leída del archivo4302020
	 */
	private static String getSaltFromFile(String keyFilePath)  {
		StringBuffer bf = new StringBuffer("");
		InputStreamReader a;
		try {
			a = new InputStreamReader( new FileInputStream(keyFilePath), "UTF-8");
			String line;
			BufferedReader br = new BufferedReader(a);
			while((line = br.readLine()) != null){
				bf.append(line.trim());
			}
			br.close();
		}
		catch (IOException e) {

		} 
		return bf.toString();
	}

}
