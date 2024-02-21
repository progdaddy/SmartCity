package smartcity;

import org.eclipse.paho.client.mqttv3.*;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;
import java.util.Arrays;

public class EMQXSubscribingClient {
	private static final String BROKER = "h15d7664.ala.us-east-1.emqxsl.com";
	private static final String PORT = "8883";
	public static final String TOPIC_SANDCATCH = "tk/sensor/logger1/sandfang";
	public static final String TOPIC_WATERLEVEL = "tk/sensor/logger1/waterlevel";    
	public static final String SANDCATCH_CLIENT_ID = "sandfang_client";
	public static final String WATERLEVEL_CLIENT_ID = "waterlevel_client";

	//scramble before upload to github
	
	private static final int CONNECT_TIMEOUT = 300;

	//When the cleanSession flag is set to true, the client explicitly requests a non-persistent session. 
	//In this scenario, if the client disconnects from the broker, all queued information and messages from the previous persistent session are discarded. 
	//The client starts with a clean slate upon reconnection.
	private static final boolean CLEAN_SESSION = true;

	//QoS 0 in MQTT offers a best-effort delivery mechanism where the sender does not expect an acknowledgment or guarantee of message delivery
	//In QoS 1 of MQTT, the focus is on ensuring message delivery at least once to the receiver. 
	//When a message is published with QoS 1, the sender keeps a copy of the message until it receives a PUBACK packet from the receiver, confirming the successful receipt. 
	//If the sender doesn’t receive the PUBACK packet within a reasonable time frame, it re-transmits the message to ensure its delivery.
	//QoS 2 offers the highest level of service in MQTT, ensuring that each message is delivered exactly once to the intended recipients. 
	//To achieve this, QoS 2 involves a four-part handshake between the sender and receiver.
	private static final int QoS = 1;
	// private static final String CA_CERT_PATH = EMQXSubscribingClient.class.getResource("/emqxsl-ca.crt").getPath();

    public static void main(String args[]) {
    	
    	startSandCatchClient(SANDCATCH_CLIENT_ID);
    	startWaterLevelClient(WATERLEVEL_CLIENT_ID);
        
    }
	
	private static void startSandCatchClient(String sandCatchClientId) {

		MqttClient client = null;
		try {
			String server = "ssl://" + BROKER + ":" + PORT;
			client = new MqttClient(server, sandCatchClientId);
			
			MqttConnectOptions options = new MqttConnectOptions();
			options.setUserName(USERNAME);
			options.setPassword(PASSWORD.toCharArray());
			options.setConnectionTimeout(CONNECT_TIMEOUT);
			options.setCleanSession(CLEAN_SESSION);
			// options.setSocketFactory(getSocketFactory(CA_CERT_PATH));

			System.out.println("Connecting to broker: " + server + " with client ID: " + sandCatchClientId);
			client.connect(options);

			if (!client.isConnected()) {
				System.out.println("Failed to connect to broker: " + server);
				return;
			}

			System.out.println("Connected to broker: " + server);
			
            client.subscribe(TOPIC_SANDCATCH, QoS);
            System.out.println("Subscribed to topic: " + TOPIC_SANDCATCH);
            
            client.setCallback(new MqttCallback() {

                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println(MessageFormat.format("Connection lost. Cause: {0}", cause));
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    System.out.println(MessageFormat.format("Callback: received message from topic {0}: {1}",
                            topic, message.toString()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    try {
                        System.out.println(MessageFormat.format("Callback: delivered message to topics {0}",
                                Arrays.asList(token.getTopics())));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            });


		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (client != null) {
				try {
					client.close();
				} catch (MqttException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void startWaterLevelClient(String waterLevelClientId) {

		MqttClient client = null;
		try {
			String server = "ssl://" + BROKER + ":" + PORT;
			client = new MqttClient(server, waterLevelClientId);
			
			MqttConnectOptions options = new MqttConnectOptions();
			options.setUserName(USERNAME);
			options.setPassword(PASSWORD.toCharArray());
			options.setConnectionTimeout(CONNECT_TIMEOUT);
			options.setCleanSession(CLEAN_SESSION);
			// options.setSocketFactory(getSocketFactory(CA_CERT_PATH));

			System.out.println("Connecting to broker: " + server + " with client ID: " + waterLevelClientId);
			client.connect(options);

			if (!client.isConnected()) {
				System.out.println("Failed to connect to broker: " + server);
				return;
			}

			System.out.println("Connected to broker: " + server);
			
            client.subscribe(TOPIC_WATERLEVEL, QoS);
            System.out.println("Subscribed to topic: " + TOPIC_WATERLEVEL);
            
            client.setCallback(new MqttCallback() {

                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println(MessageFormat.format("Connection lost. Cause: {0}", cause));
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    System.out.println(MessageFormat.format("Callback: received message from topic {0}: {1}",
                            topic, message.toString()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {

                }

            });


		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (client != null) {
				try {
					client.close();
				} catch (MqttException e) {
					e.printStackTrace();
				}
			}
		}
	}

    public static SSLSocketFactory getSocketFactory(String caCertPath) throws Exception {
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");

        // load CA certificate into keystore to authenticate server
        Certificate caCert = certFactory.generateCertificate(new FileInputStream(caCertPath));
        X509Certificate x509CaCert = (X509Certificate) caCert;

        KeyStore caKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        caKeyStore.load(null, null);
        caKeyStore.setCertificateEntry("cacert", x509CaCert);

        TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmFactory.init(caKeyStore);

        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(null, tmFactory.getTrustManagers(), null);

        return sslContext.getSocketFactory();
    }
}