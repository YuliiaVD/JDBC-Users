package org.example.app.view;

import org.example.app.utils.AppStarter;
import org.example.app.utils.Constants;

import java.util.InputMismatchException;
import java.util.Scanner;

public class AppView {

    public int getOption() {
        Scanner scanner = new Scanner(System.in);
        int option = 0;
        getMenu();
        try {
            option = scanner.nextInt();
        } catch (InputMismatchException e) {
            System.out.println(Constants.INCORRECT_VALUE_MSG);
            AppStarter.startApp();
        }
        return option;
    }

    private void getMenu() {
        System.out.print("""                
                
                OPTIONS:
                1 - Create contact.
                2 - Read contacts.
                3 - Update contact.
                4 - Delete contact.
                5 - Read contact by id.
                0 - Close the App.
                """);
        System.out.print("Input your option: ");
    }

    public void getOutput(String output) {
        if (output.equals("0")) {
            System.out.println(Constants.APP_CLOSE_MSG);
            System.exit(0);
        } else System.out.println(output);
    }

}
