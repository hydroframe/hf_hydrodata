"""
Interface classes to manage versions of machine learning models and to execute
machine learning models.
"""
# pylint: disable=C0301, R0903, R0913

from typing import List


class ModelRepository:
    """
    Interface for a model repository to manage machine learning models and their implementation code.
    This is an abstract interface that must be implemented by a ModelRepository implementation.

    Example to execute hydrogen evaluator:

    .. code-block:: python

        executor = model_repository.get_model_implementation_class("hydrogen_evaluator", "1.0.2")
        try:
            parameters = {
                "model_name": "hydrogen_evaluator",
                "model_version": "1.0.2",
                "model_repository": model_repository,
                "shared_temp_folder": ".",
                "current_conditions_path": "./current_conditions.nc",
                "forcing_ensemble_files": [
                    "./run1.1990-08-04_1990-11-02.nc",
                    "./run2.2015-08-04_2015-11-02.nc",
                    "./run3.1994-08-04_1994-11-02.nc",
                    "./run4.2000-08-04_2000-11-02.nc"
                ],
                "forecast_duration": 90,
                "history_duration": 90,
                "scenario_start_date", "2023-08-07"
                "output_file": "./result.nc"
            executor.execute(parameters)
        except Exception:
            logging.exception("Failure while executing model")


    Example to register a model version during training time.

    .. code-block:: python
        model_repository.save_model_file("hydrogen_evaluator", "1.0.2", "model", "multilstm_sept8.pt")
        model_repository.save_model_file("hydrogen_evaluator", "1.0.2", "scalers", "conus1.scalers")
        model_repository.save_model_file("hydrogen_evaluator", "1.0.2", "surface_config", "surface_config.json")
        model_repository.save_model_file("hydrogen_evaluator", "1.0.2", "subsurface_config", "subsurface_config.json")
        model_repository.set_model_implementation_class("hydrogen_evaluator", "1.0.2", "lossunis.EvaluatorImplementation ")

    Note when executing a model it is the responsibility of the caller to pip install the version of the model implementation class
    into the python virtual environment. Different versions may be in different packages or may have different class names in the same package.
    Training attributes may be associated with model versions that may be useful at training time.
    Each model version may register with the reposistory various model files required by the version of the implementation.
    """

    def get_model_file(
        self,
        model_name: str,
        version: str,
        file_key: str,
        output_file_path: str,
        if_not_exists=True,
    ):
        """
        Get the contents of a model file and store it in the output_file_path.

        Parameters:
            model_name:         The name of a model that was saved with save_model_file method.
            version:            A model version saved with save_model_file method.
            file_key:           Identifies the type of model file saved in the repository.
            output_file_path:   The file path to save the model retrieved from the model repository.
            if_not_exists:      If True then do not get the file again if it already exists in output_file_path.

        Example calls from an evaluator to get required model files from the repository:

        .. code-block:: python
            model_repository.get_model_file("hydrogen_evaluator", "1.0.2", "model", "model.pt")
            model_repository.get_model_file("hydrogen_evaluator", "1.0.2", "subsurface_config", "subsurface.config.json")
        """

    def save_model_file(
        self, model_name: str, version: str, file_key: str, model_file_path: str
    ):
        """
        Save a model into a model repository.

        Parameters:
            model_name:         The name of a model to be saved.
            version:            The model version to be saved.
            file_key:           Identifies the type of model file saved in the repository.
            model_file_path:    Path name to a model file to be saved into the repository.

        Example calls during model training to save model files required to execute the model:

        .. code-block:: python
            model_repository.save_model_file("hydrogen_evaluator", "1.0.2", "model", "multistm_sept8.pt")
            model_repository.save_model_file("hydrogen_evaluator", "1.0.2", "subsurface_config", "subsurface.config.json")
        """

    def save_attributes(self, model_name: str, version: str, attributes: dict):
        """
        Save or update attributes of a model version in the repository.

        Parameters:
            model_name:         The name of a model,
            version:            The model version.
            attributes:         A dict of attribute values to be associated the the model version.
        The model attributes are created or updated. Any previously saved attributes not specified are not changed.

        Example calls during model training to associate attributes with a model version:

        .. code-block:: python
            model_repository.save_attributes("hydrogen_evaluator", "1.0.2", {accuracy": "0.65", "loss": "0.5"})
            model_repository.save_attributes("hydrogen_evaluator", "1.0.2", {"released": "true"})
            model_repository.save_attributes("hydrogen_evaluator", "1.0.2", {"repo": "git+ssh://git@github.com/HydroFrame-ML/hydrogen-emulator-los-sunis.git@0.0.0"})
        """

    def get_attributes(self, model_name: str, version: str) -> dict:
        """
        Get all the attributes of a model version in the repository.

        Parameters:
            model_name:         The name of a model,
            version:            The model version.
        Returns:
            A dict with all the attributes associated with the model version.

        Example to get attributes of a model version.

        .. code-block:: python

            attributes = model_repository.get_attributes("hydrogen_evaluator", "1.0.2")
            assert attributes.get("released") == "true"
            assert attributes.get("loss") == "0.5"

        """

    def get_versions(self, model_name: str) -> List[str]:
        """
        Get the list of versions of the model name.

        Parameters:
            model_name:     Name of a model.
        Returns:
            A list of all the versions of the model.

        Example to get versions of a model:

        .. code-block:: python

            versions = model_repository.get_versions("hydrogen_evaluator")
            assert versions = ["1.0.1", "1.0.2"]
        """

    def get_model_implementation_class(self, model_name: str, version: str):
        """
        Get the implementation class of the model version.

        Parameters:
            model_name:         The name of a model,
            version:            The model version.
        Returns:
            A class that implements the ModelExecutionInterface interface.

        Example to get the implementation class of a model.

        .. code-block:: python

            executor = model_repository.get_model_implementation_class("hydrogen_evaluator", "1.0.2")
            parameters = {}
            executor.execute(parameters)

        """

    def set_model_implementation_class(
        self, model_name: str, version: str, class_package_name: str
    ):
        """
        Set the class_package_name of the the implementation class of the model version.

        Parameters:
            model_name:         The name of a model,
            version:            The model version.
            class_package_name: The fully qualified package name of the implementation class.
        The class_package_name must be a class that implements the ModelExecutionInterface.

        Example to register the implementation class of a model version.

        .. code-block:: python

            model_repository.set_model_implementation_class("hydrogen_evaluator", "1.0.2", "losunis.EvaluatorImplementation")
        """


class ModelExecutionInterface:
    """
    Abstract interface class for implementations of of a model.
    This is an abstract interface that must be implemented by the implementation code that evaluates a model.
    """

    def execute(self, parameters: dict):
        """
        Executes the model.

        Parameters:
            parameters: A dict containing the input parameters for the execution of the model.

        Note the parameters may be different for different model names.
        Parameters names should be backward compatible between different versions of a model name.
        """
