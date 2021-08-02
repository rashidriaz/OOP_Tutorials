package rashid.proj.ioProject;

import java.util.*;

public class Main {
    private static Locations locations = new Locations();
    private static final Scanner input = new Scanner(System.in);
    public static void main(String[] args) {

        //-------------------------------------------------------------------------------//
        Map<String, String> vocabulary = new HashMap<>();
        vocabulary.put("NORTH", "N");
        vocabulary.put("WEST", "W");
        vocabulary.put("EAST", "E");
        vocabulary.put("SOUTH", "S");
        vocabulary.put("QUIT", "Q");
        //---------------------------------------------------------------------------------//

        int loc = 1; 
	while(true){
        System.out.println(locations.get(loc).getDescription());
	    if(loc == 0) {
            break;
        }
	    Map<String, Integer> exists = locations.get(loc).getExits();
        System.out.print("Available Exists are : ");
        for(String exits : exists.keySet()){
            System.out.print(exits + ", ");
        }
        System.out.println();
        String direction = input.nextLine().toUpperCase();
        if(direction.length() > 1){
            String[] words = direction.split(" ");
            for (String word : words) {
                if (vocabulary.containsKey(word)){
                    direction = vocabulary.get(word);
                    break;
                }
            }
        }
        if(exists.containsKey(direction)) {
            loc = exists.get(direction);
        }else{
            System.out.println("You cannot go to that direction");
        }
    }
    }
}