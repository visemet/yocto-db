import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

/**
* Creates fake data for testing purposes. Prints out formatted tuples to be read
* by Erlang IO.
* 
* @author anjoola
*/
public class DataGenerator {
    
    /** Random number to seed */
    private static Random random = new Random(1337133L);
    
    /** Valid characters for atoms */
    private static char[] VALID_CHARS = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
        'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0',
        '1', '2', '3', '4', '5', '6', '7', '8', '9', '_'};
    
    /** Arraylist containing the results */
    private static ArrayList<ArrayList<String>> results;
    
    /** Strint to print */
    private static StringBuilder toPrint = new StringBuilder();
    
    public static void main(String[] args) {   
        int numRows = 0, numCols = 0;
        String outFile, type = null;
        Scanner in = new Scanner(System.in);
        
        // Starting message and prompt
        System.out.println("+----------------------------------------------------------------------------+");
        System.out.println("|                          DATA GENERATOR version 1                          |");
        System.out.println("+----------------------------------------------------------------------------+");
        System.out.println("| Define the schema. Can have columns with types INT, DOUBLE, or VARCHAR.    |");
        System.out.println("| Correct parameters for each type must be specified like below:             |");
        System.out.println("|                                                                            |");
        System.out.println("| INT(min,max[,step]) - Generates an integer column with values ranging from |");
        System.out.println("|                       min to max with an optional parameter step that      |");
        System.out.println("|                       indicates the step size.                             |");
        System.out.println("| DOUBLE(min,max)     - Generates a double with ranging from min to max      |");
        System.out.println("| CHAR(size)          - Generates a string of length size                    |");
        System.out.println("| ATOM(size)          - Generates an atom of length size                     |");
        System.out.println("| ATOM(goog,aapl,...) - Selects a random atom from the given choices         |");
        System.out.println("+----------------------------------------------------------------------------+\n");
        
        System.out.print("\nOutput filename: ");
        outFile = in.next();
        
        try {
            System.out.print("Number of columns: ");
            numCols = Integer.parseInt(in.next());
            
            if (numCols < 1)
                throw new NumberFormatException(null);
            
            System.out.print("Number of tuples: ");
            numRows = Integer.parseInt(in.next());
            
            if (numRows < 1)
                throw new NumberFormatException(null);
        } 
        catch (NumberFormatException e) {
            System.err.println("ERROR! Numbers must be positive integers!");
            System.exit(1);
        }
        
        results = new ArrayList<ArrayList<String>>();
        // Loop through all of the columns and rows
        for (int i = 0; i < numCols; i++) {
            ArrayList<String> col = new ArrayList<String>();
            
            System.out.print("\nColumn " + (i + 1) + " Name: ");
            String colName = in.next();
            
            try {
                System.out.print("Type of '" + colName + "': ");
                type = in.next();
                generateRows(type, numRows, col);
            } catch (InputMismatchException e) {
                System.err.println("ERROR! Type must be one of INT, DOUBLE, CHAR, or ATOM and must specify their parameters!");
                System.exit(1);
            }
            results.add(col);
        }
        // Print the resulting tuples
        ArrayList<String> stuff;
        // Loop through the rows
        for (int i = 0; i < numRows; i++) {
            print("{");
            
            // Print each column in the row
            for (int j = 0; j < numCols - 1; j++) {
                stuff = results.get(j);
                print(stuff.get(i) + ", ");
            }
            
            stuff = results.get(results.size() - 1);
            print(stuff.get(i) + "}.\n");
        }
        
        // TODO
        // Print out the resulting thingy
        System.out.println("\n\n" + toPrint.toString());
        
        // Write to a file
        writeToFile(outFile);
    }
    
    
    /** 
     * Generate the rows for a given column and its type
     * 
     * @param input the type of the column
     * @param numRows the number of rows
     * @param result the list of all the rows
     */
    private static void generateRows(String input, int numRows, ArrayList<String> result) {
        String parse = new String(input);
        int min = 0, max = 0, step = 0, length = 0;
        double minD = 0, maxD = 0;
        
        // Parse the type
        int index = parse.indexOf("(");
        String type = parse.substring(0, index);
        
        // INT(min,max,step) or INTEGER(min,max,step)
        if (type.equals("INT") || type.equals("INTEGER")) {
            // Get the min
            parse = parse.substring(index + 1);
            index = parse.indexOf(",");
            min = Integer.parseInt(parse.substring(0, index));
            
            // Get the max
            parse = parse.substring(index + 1);
            index = parse.indexOf(",");
            
            // Step size was not specified
            if (index == -1) {
                index = parse.indexOf(")");
                max = Integer.parseInt(parse.substring(0, index));
                step = -1;
            }
            else {
                max = Integer.parseInt(parse.substring(0, index));
            
                // Get the step size (optional parameter)
                parse = parse.substring(index + 1);
                index = parse.indexOf(")");
                step = Integer.parseInt(parse.substring(0, index));
            }
            genRandomInt(min, max, step, numRows, result);
        }
        // DOUBLE(min,max)
        else if (type.equals("DOUBLE")) {
            // Get the min
            parse = parse.substring(index + 1);
            index = parse.indexOf(",");
            minD = Double.parseDouble(parse.substring(0, index));
            
            // Get the max
            parse = parse.substring(index + 1);
            index = parse.indexOf(")");
            maxD = Double.parseDouble(parse.substring(0, index));
            genRandomDouble(minD, maxD, numRows, result);
        }
        // CHAR(length)
        else if (type.equals("CHAR")) {
            // Get the length
            parse = parse.substring(index + 1);
            index = parse.indexOf(")");
            length = Integer.parseInt(parse.substring(0, index));
            
            genRandomChar(length, numRows, result);
        }
        // ATOM(length) or ATOM(a,b,c,...)
        else if (type.equals("ATOM")) {
            // Get the length
            parse = parse.substring(index + 1);
            index = parse.indexOf(")");
            // See if it was a number for length or actual atoms to choose from
            try {
                length = Integer.parseInt(parse.substring(0, index));
                genRandomAtom(length, numRows, result);
            } catch(NumberFormatException e) {
                parse = parse.substring(0, index);
                
                // If it was actually a list of atoms
                ArrayList<String> atoms = new ArrayList<String>();
                while ((index = parse.indexOf(",")) != -1) {
                    atoms.add(parse.substring(0, index));
                    parse = parse.substring(index + 1);
                }
                // Add the last atom
                atoms.add(parse);
                genRandomAtom(atoms, numRows, result);
            }
        }
        else 
            throw new InputMismatchException(null);
    }
    
    
    /**
     * Generates a random integer in the range specified with the step values
     * specified
     *
     * @param min the minimum value
     * @param max the maximum value
     * @param step the step size
     * @param numRows the number of rows
     * @param results the array containing the results
     */
    private static void genRandomInt(int min, int max, int step, int numRows, ArrayList<String> results) {
        int result = 0;
        
        // Check for valid parameters
        if (min >= max) {
            System.err.println("ERROR! Must specify a valid range!");
            System.exit(1);
        }
        else if (step > (max - min)) {
            System.err.println("ERROR! Step size cannot be larger than the range!");
            System.exit(1);
        }
        
        for (int i = 0; i < numRows; i++) {
            if (step != -1)
                result = min + random.nextInt((max - min) / step) * step;
            else
                result = min + random.nextInt(max - min);
            results.add(String.valueOf(result));
        }
    }
    
    
    /**
     * Generates a random double in the range specified with the step values
     * specified
     *
     * @param min the minimum value
     * @param max the maximum value
     * @param numRows the number of rows
     * @param results the array containing the results
     */
    private static void genRandomDouble(double min, double max, int numRows, ArrayList<String> results) { 
        double result = 0.0;
        
        // Check for valid parameters
        if (min >= max) {
            System.err.println("ERROR! Must specify a valid range!");
            System.exit(1);
        }
        
        for (int i = 0; i < numRows; i++) {
            result = min + random.nextDouble() * (max - min);
            results.add(String.valueOf(result));
        }
    }
    
    
    /**
     * Generates a random string with the length specified
     *
     * @param length the length of the string
     * @param numRows the number of rows
     * @param results the array containing the result
     */
    private static void genRandomChar(int length, int numRows, ArrayList<String> results) {
        StringBuilder result;
        int randLength;
        
        // Check for valid parameters
        if (length < 1) {
            System.err.println("ERROR! Length must be at least 1!");
            System.exit(1);
        }
        
        for (int i = 0; i < numRows; i++) {
            result = new StringBuilder("'");
            
            // Randomly generates each letter
            for (int j = 0; j < length; j++) {
                int caps = random.nextInt(1);
                String letter = "" + (char)(random.nextInt(26) + 97);
                
                // Randomly decides to capitalize or not
                letter = caps == 1 ? letter.toUpperCase() : letter.toLowerCase();
                result.append(letter);
            }
            result.append("'");
            results.add(result.toString());
        }
    }
    
    
    /**
     * Generates a random atom with the length specified
     *
     * @param length the length of the atom
     * @param numRows the number of rows
     * @param results the array containing the result
     */
    private static void genRandomAtom(int length, int numRows, ArrayList<String> results) {
        StringBuilder result;
        
        // Check for valid parameters
        if (length < 1) {
            System.err.println("ERROR! Length must be at least 1!");
            System.exit(1);
        }
        
        for (int i = 0; i < numRows; i++) {
            result = new StringBuilder("");
            
            // Randomly generates each letter
            for (int j = 0; j < length; j++) {
                String letter = "" + VALID_CHARS[(random.nextInt(63))];
                result.append(letter);
            }
            results.add(result.toString());
        }
    }
    
    
    /**
     * Generates an atom randomly selected from a list of atoms
     *
     * @param atoms the list of valid atoms
     * @param numRows the number of rows
     * @param results the array containing the result
     */
    private static void genRandomAtom(ArrayList<String> atoms, int numRows, ArrayList<String> results) {
        for (int i = 0; i < numRows; i++) {
            // Randomly pick an atom to use
            String atom = atoms.get(random.nextInt(atoms.size()));
            results.add(atom);
        }
    }
    
    
    /**
     * Appends the results to be printed, which will later be printed to a 
     * text file
     */
    private static void print(String str) {
        toPrint.append(str);
    }
    
    
    /** 
     * Writes table information to the file
     *
     * @param fileName the name of the file to write out to
     */
    private static void writeToFile(String fileName) {
        try {
            FileWriter fw = new FileWriter(fileName + ".dta");
            BufferedWriter out = new BufferedWriter(fw);
            out.write(toPrint.toString());
            out.close();
        }
        // If unable to write to a file, output to stdout instead
        catch (Exception e) {
            System.err.println("ERROR! Could not write to file. Here is the output instead: \n\n");
            System.out.println(toPrint.toString());
        }
    }
}
