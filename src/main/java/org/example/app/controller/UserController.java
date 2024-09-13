package org.example.app.controller;

import org.example.app.service.UserService;
import org.example.app.utils.AppStarter;
import org.example.app.view.*;

public class UserController {

    UserService service = new UserService();

    public void create() {
        UserCreateView view = new UserCreateView();
        view.getOutput(service.create(view.getData()));
        AppStarter.startApp();
    }

    public void read() {
        UserReadView view = new UserReadView();
        view.getOutput(service.read());
        AppStarter.startApp();
    }

    public void update() {
        UserUpdateView view = new UserUpdateView();
        view.getOutput(service.update(view.getData()));
        AppStarter.startApp();
    }

    public void delete() {
        UserDeleteView view = new UserDeleteView();
        view.getOutput(service.delete(view.getData()));
        AppStarter.startApp();
    }

    public void readById() {
        UserReadByIdView view = new UserReadByIdView();
        view.getOutput(service.readById(view.getData()));
        AppStarter.startApp();
    }
}
