

import java.io.FileInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 
 * @author Irmak Demir
 * @author Berrak Taşkınsu
 */
public class Sender {

	public static float packageLossProbability;
	public static Random random = new Random();
	public static int fileSizeInBits;
	// Global Variables
	// Shared Variables
	public static int base;
	public static DatagramSocket udpSocket;
	public static Timer timer;
	public static boolean endOfTransmission;
	public static boolean retransmitPackets;
	public static int segmentNumber;
	
	// Argument Variables (Unchanged)
	public static int windowSize;
	public static int numberOfPackets;
	public static long timeout;

	// Constants
	private static final int packetSize = 1024;
	private static final int headerSize = 2;
	private static final int sleepTime = 10000;
	private static final int sleepTimeShort = 100;

	/**
	 * Inner Class Packet
	 */
	private class Packet {

		// Private Member Fields
		private int sequenceNumber;
		private byte[] data;
		private ByteBuffer udpSegment;
		private int dataSize;

		/**
		 * Constructor of Class Packet
		 * 
		 * @param data
		 * @param sequenceNumber
		 * @param dataSize
		 */
		public Packet(byte[] data, int sequenceNumber, int dataSize) {

			this.sequenceNumber = sequenceNumber % (int) Math.pow(2, 16);
			this.dataSize = dataSize;
			this.data = new byte[packetSize - headerSize];
			for (int i = 0; i < data.length; i++) {
				this.data[i] = data[i];
			}
			this.udpSegment = ByteBuffer.allocate(packetSize);
			udpSegment.order(ByteOrder.BIG_ENDIAN).putShort((short) sequenceNumber).put(this.data);
		}

		/**
		 * Getter for Member Field sequenceNumber
		 * 
		 * @return the integer sequence number of the packet object
		 */
		public int getSequenceNumber() {
			return sequenceNumber;
		}

		/**
		 * Getter for Member Field dataSize
		 * 
		 * @return the actual number of bytes in the packet corresponding to data
		 */
		public int getDataSize() {
			return dataSize;
		}

		/**
		 * Getter for Member Field udpSegment
		 * 
		 * @return an array of bytes which is the UDP segment containing header and
		 *         data.
		 */
		public byte[] getUDPSegment() {
			return udpSegment.array();
		}
	}

	/**
	 * Inner Class PacketSender
	 */
	private class PacketSender {

		// Public Member Fields
		public Thread thread;

		// Private Member Fields
		private ArrayList<Packet> packets;
		private InetAddress ipAddress;
		private int portNumber;

		/**
		 * Constructor of Inner Class PacketSender
		 * 
		 * @param packets
		 * @param ipAddress
		 * @param portNumber
		 * @throws SocketException
		 */
		public PacketSender(ArrayList<Packet> packets, InetAddress ipAddress, int portNumber) throws SocketException {
			this.packets = new ArrayList<Packet>();
			for (Packet packet : packets) {
				this.packets.add(packet);
			}
			this.ipAddress = ipAddress;
			this.portNumber = portNumber;

		}

		/**
		 * Method start
		 */
		public void start() {
			if (thread == null) {
				thread = new Thread() {

					public void run() {
						base = 1;
						try {
							while (true) {
								
								// Send All Packets Within the Window
								for (segmentNumber = base; segmentNumber < base + windowSize; segmentNumber++) {
									
									if (segmentNumber > numberOfPackets) {
										break;
									}
									if (retransmitPackets) {
										//System.out.println("Retransmitting packets. SN: " + segmentNumber);
										retransmitPackets = false;
										segmentNumber = base - 1;
										continue;
									}
									if (random.nextFloat() >= packageLossProbability) {
										sendPacket(packets.get(segmentNumber - 1));
										//System.out.println("Packet#" + segmentNumber + " sent.");
									}
								}
								
								Thread.sleep(sleepTimeShort);
							}
						}

						// Stop If Main Method Interrupts This Thread (When All Acknowledgement Are
						// Received)
						catch (InterruptedException | IOException e) {
							return;
						}
					}
				};
				thread.start();
			}
		}

		/**
		 * Method sendPacket
		 * 
		 * @param packet
		 * @throws IOException
		 */
		public void sendPacket(Packet packet) throws IOException {
			DatagramPacket udpPacket = new DatagramPacket(packet.getUDPSegment(), packet.getUDPSegment().length,
					ipAddress, portNumber);
			udpSocket.send(udpPacket);
		}
	}

	/**
	 * Inner Class ACKReceiver
	 */
	private class ACKReceiver {

		public Thread thread;

		/**
		 * Constructor of Inner Class ACKReceiver
		 */
		public ACKReceiver() {

		}

		/**
		 * Method start
		 */
		public void start() {
			if (thread == null) {
				thread = new Thread() {
					public void run() {
						try {
							while (true) {
								// Until All Acknowledgments Are Received
								if (base <= numberOfPackets) {
									// Receive ACK
									short ack = receivePacket();
									
									// If ACK is Within the Window, Update Window
									if (ack >= base && ack <= base + windowSize /*&& segmentNumber >= ack*/) {
										base = ack + 1; // DELETED +1
										restartTimer();
										//System.out.println("Timer Restarted");
										
									}
									//System.out.println("ACK#" + ack + " received. Base: " + base);
									
								}
								// Sleep (Most Probably Interrupted in 1st Iteration)
								else {
									
									Thread.sleep(sleepTime);
								}
								
							}
						}

						// Stop If Main Method Interrupts This Thread (When All Acknowledgement Are
						// Received)
						catch (IOException | InterruptedException e) {
							return;
						}
					}
				};
				thread.start();
			}
		}

		/**
		 * Method receivePacket
		 * 
		 * @return integer representing the sequence number of the received ACK packet.
		 * @throws IOException
		 */
		public short receivePacket() throws IOException {
			byte[] buffer = new byte[packetSize];
			DatagramPacket udpPacket = new DatagramPacket(buffer, buffer.length);
			udpSocket.receive(udpPacket);
			ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
			short segNum = byteBuffer.getShort();
			return segNum;
		}
	}

	/**
	 * Method createPackets()
	 * 
	 * @param filePath string path of the file to be copied
	 * @return an ArrayList of packets consisting of all packets the PacketSender
	 *         thread will send
	 */
	public ArrayList<Packet> createPackets(String filePath) {

		FileInputStream fileInputStream;
		ArrayList<Packet> packets = new ArrayList<Packet>();
		fileSizeInBits = 0;
		try {
			// Open fileInfutStream
			fileInputStream = new FileInputStream(filePath);

			// Read file into packets
			int currentSequenceNumber = 1;
			byte[] dataBuffer = new byte[packetSize - headerSize];
			int numRead = fileInputStream.read(dataBuffer);
			while (numRead != -1) {
				packets.add(new Packet(dataBuffer, currentSequenceNumber, numRead));
				numRead = fileInputStream.read(dataBuffer);
				currentSequenceNumber++;
				fileSizeInBits += numRead;
			}
			
			fileInputStream.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return packets;
	}

	/**
	 * Method restartTimer()
	 */
	public static synchronized void restartTimer() {
		if (timer != null) {
			timer.cancel();
			timer.purge();
		}
		timer = new Timer();
		
		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				timer.cancel();
				timer.purge();
				if (!endOfTransmission) {
					//System.out.println("Timeout Occurred, Base:" + base);
					retransmitPackets = true;
				}
			}
			
		}, timeout);
	}
	
	/**
	 * Class Main
	 * 
	 * @param args arg0: File Path arg1: Receiver Port Number arg2: Window Size
	 *             arg3: Retransmission Timeout
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String args[]) throws InterruptedException, IOException {
		
		if (args.length != 4) {
			System.out.println("Invalid arguments");
			return;
		}
		
		

		// Initialize Global Variables
		windowSize = Integer.parseInt(args[2]);
		timeout = Integer.parseInt(args[3]);
		endOfTransmission = false;
		retransmitPackets = false;
		packageLossProbability = 0.0f;
		
		// Constants
		final String ipAddressString = "127.0.0.1";
		final int portNumber = Integer.parseInt(args[1]);
		String filePath = args[0];

		// Initialize Sender and Create Packets
		Sender sender = new Sender();
		ArrayList<Packet> packets = sender.createPackets(filePath);
		numberOfPackets = packets.size();

		// Initialize Connection
		InetAddress ipAddress = Inet4Address.getByName(ipAddressString);
		udpSocket = new DatagramSocket();

		/*System.out.println("Total number of packets: " + packets.size());
		for (Packet packet : packets) {
			System.out.println("Packet #" + packet.getSequenceNumber() + ": " + packet.getDataSize() + " bytes of data.");
		}*/

		// Initialize and Start Threads
		PacketSender packetSender = sender.new PacketSender(packets, ipAddress, portNumber);
		packetSender.start();
		ACKReceiver ackReceiver = sender.new ACKReceiver();
		ackReceiver.start();

		// Wait Until Acknowledgement of All Packets are Received
		while (base <= numberOfPackets) {
			Thread.sleep(10);
		};
		endOfTransmission = true;
		
		timer.cancel();
		timer.purge();
		timer = null;
		// Stop Threads
		packetSender.thread.interrupt();
		ackReceiver.thread.interrupt();

		// Send EOT Packet
		//System.out.println("Sending EOT Package");
		Packet packet = sender.new Packet(new byte[packetSize - headerSize], 0, 0);
		DatagramPacket udpPacket = new DatagramPacket(packet.getUDPSegment(), packet.getUDPSegment().length, ipAddress,
				portNumber);
		udpSocket.send(udpPacket);
		//System.out.println("Sent EOT Package");
		
		// End Connection
		udpSocket.disconnect();
		udpSocket.close();
		fileSizeInBits *= 8;
		System.out.println("Window Size: " + windowSize + "\tFile Size In Bits: " + fileSizeInBits);
		
		return;
	}
}
