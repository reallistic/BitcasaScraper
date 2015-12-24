BitcasaScraper
==============


# Install

## Mac OS X (El Capitan)

 - Install [homebrew](http://brew.sh)
 - Install qt `brew install qt`
 - Install libffi `brew install libffi`
 - Install openssl `brew install openssl`
 - Force link openssl `brew link openssl --force` (**IMPORTANT** force can be dangerous. You were warned)


## Centos

 - Install qt `qt qt-devel`
 - Install g++ `install gcc-c++`
 - Install python-devel `python-devel`
 - Install libffi `libffi libffi-devel`
 - Install openssl `openssl openssl-devel`
 - Install libxml `libxslt-devel libxml2 libxml2-devel`
 - Install webkit_server `qt-webkit-devel`
 - Install xvfb `xorg-x11-server-Xvfb`
 
```
    sudo yum install qt qt-devel \
        gcc-c++ \
       	python-devel \
        libffi libffi-devel \
        openssl openssl-devel \
        libxslt-devel libxml2 libxml2-devel \
        qt-webkit-devel \
        xorg-x11-server-Xvfb
```


## Authenticating with bitcasa
To authenticate run the following command:
```
    python bitcasatools.py authenticate --username "<bitcasa email>" --password "<bitcasa password>"
```

 on systems (like centos) that need an explicit X server running you may need to use this command instead:
 
```
    xvfb-run python bitcasatools.py authenticate --username "<bitcasa email>" --password "<bitcasa password>"
```


## Notes
 - If you receive the error `src/webkit_server file or directory not found` then [check here](https://github.com/thoughtbot/capybara-webkit/wiki/Installing-Qt-and-compiling-capybara-webkit) for more info on compiling `webkit_server`.
 - If you still get `qmake command not found` [check here](http://stackoverflow.com/a/18225282/1991100)
 - If you receive errors that `No X server is available`, prefix your authentication command with `xvfb-run`
 - See [this thread](https://github.com/pyca/cryptography/issues/693) if you run into `libffi` trouble
