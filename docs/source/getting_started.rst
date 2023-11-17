.. _getting_started:

Getting Started
======================

Installation
------------
The best way to install ``hf_hydrodata`` is using pip. This installs our 
latest stable release with fully-supported features. ::

    pip install hf_hydrodata

You can also install the latest development version by cloning the GitHub repository and using pip
to install from the local directory::  

    pip install git+https://github.com/hydroframe/hf_hydrodata.git


Creating a HydroGEN API Account
----------------------------------
Users must create a HydroGEN API account and register their PIN before using the 
``hf_hydrodata`` package.

First, please visit our `HydroGEN PIN Page <https://hydrogen.princeton.edu/pin>`_ to 
sign up for an account and create a 4-digit PIN.

After creating your PIN, you must register that PIN on the machine that you intend
to use. You can run the following code one time to register your PIN::  

    from hf_hydrodata.gridded import register_api_pin
    register_api_pin("<your_email>", "<your_pin>")

Your PIN will expire after 2 days of non-use. If your PIN expires, you must return to
the `HydroGEN PIN Page <https://hydrogen.princeton.edu/pin>`_ and create a new PIN. 
You only need to re-register this PIN with the ``register_api_pin`` method if the 
new 4-digit PIN is different from your previous 4-digit PIN (the PIN is allowed
to stay the same).
