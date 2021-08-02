package server.echo.java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Echoer extends Thread{
    private Socket socket;

    public Echoer(Socket socket){
        this.socket = socket;
    }

    @Override
    public void run() {
      try {
          BufferedReader input = new BufferedReader(
                  new InputStreamReader(socket.getInputStream()));
          PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
          while (true){
              String echoString = input.readLine();
              if (echoString.equalsIgnoreCase("exit")){
                  break;
              }
              writer.println("Echo from server : " + echoString);
          }
      }catch (IOException e){
          System.out.println("Oops : " + e.getMessage());
      }finally {
          try {
              socket.close();
          }catch (IOException e2){
              //oh well!!
          }
      }
    }
}
