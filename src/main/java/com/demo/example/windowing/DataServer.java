
package com.demo.example.windowing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.net.ServerSocket;
import java.net.Socket;

public class DataServer {

    public static void main(String[] args) {

        // STEP 1. 실행 파라미터 확인
        String inputFile = null;
        int port = 9090;

        for (int i = 0; i < args.length; i++) {

            if ("--input".equals(args[i]) && i + 1 < args.length) {
                inputFile = args[++i];

            } else if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            }
        }

        // STEP 2. 입력 파일명 필수 확인
        if (inputFile == null || inputFile.isBlank()) {
            System.err.println(
                "Usage: java DataServer " +
                "--input <file-path> [--port <port>]"
            );
            System.exit(1);
        }

        // STEP 3. 파일 존재 및 읽기 가능 여부 확인
        Path filePath = Paths.get(inputFile);

        if (!Files.isRegularFile(filePath)
                || !Files.isReadable(filePath)) {

            System.err.println(
                "Input file not found or not readable: "
                + filePath.toAbsolutePath()
            );
            System.exit(1);
        }

        // STEP 4. Socket Server 시작
        System.out.println("================================");
        System.out.println("DataServer Started");
        System.out.println("Input File : " + filePath.toAbsolutePath());
        System.out.println("Port       : " + port);
        System.out.println("================================");

        try (ServerSocket listener = new ServerSocket(port)) {

            System.out.println("Waiting for Flink connection...");

            // STEP 5. Flink Client 접속 대기
            try (Socket socket = listener.accept();
                 BufferedReader br =
                     Files.newBufferedReader(filePath);
                 PrintWriter out = new PrintWriter(
                     socket.getOutputStream(), true)) {

                System.out.println(
                    "Got new connection: " + socket
                );

                // STEP 6. 파일 데이터를 한 줄씩 전송
                String line;
                int count = 0;

                while ((line = br.readLine()) != null) {

                    out.println(line);
                    count++;

                    System.out.println(
                        "[" + count + "] " + line
                    );

                    // 데이터 전송 간격: 50ms
                    Thread.sleep(50);
                }

                System.out.println(
                    "Data transmission completed. " +
                    "Total records: " + count
                );

            }

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
            System.err.println("Data transmission interrupted.");

        } catch (IOException e) {

            System.err.println(
                "DataServer error: " + e.getMessage()
            );
        }
    }
}
