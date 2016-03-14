/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package resources;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class CleanDirectory {

    public static void cleanDirectory(Path path) {
    
        try {
            Files.deleteIfExists(path);
        } catch (IOException ex) {
            System.out.println("Clean Directory failed: " + ex);
        }

    }
    
}
