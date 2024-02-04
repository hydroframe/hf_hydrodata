"""
Interface classes to manage versions of machine learning models and to execute
machine learning models.
"""
# pylint: disable=C0301, R0903, R0913

from typing import List

def get_model_repository(options:dict=None)->ModelRepository:
    """
    Get an implementation of the model repository interface.
    
    Parameters:
        options:    A dict containing options to configure the model repository.
    
    By default the repository is stored in a directory specified by the
    environment variable 'ModelRepositoryDir' with a default value '~/.model_repo'.
    
    Other options can configure other methods to store the model repository.

    Possible Option Attributes:
        db_connection_string:   A database connection string to store model in a SQL db.
        mlflow_url:             A URL to a MLFlow web server.
        api_url:                A URL to a hf_hydrodata server to save and store models.

    Example:
    
    .. code-block:: python

        import hf_model_repository as mr
        model_repository = mr.get_model_repository()
    """


class ModelRepository:
    """
    Interface for a model repository to manage machine learning models and their implementation code.
    This is an abstract interface that must be implemented by a ModelRepository implementation.

    Example to execute hydrogen emulator:

    .. code-block:: python

        executor = model_repository.get_model_implementation_class("hydrogen_emulator", "1.0.2")
        try:
            parameters = {
                "model_name": "hydrogen_emulator,
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
        model_repository.save_model_file("hydrogen_emulator", "1.0.2", "model", "multilstm_sept8.pt")
        model_repository.save_model_file("hydrogen_emulator", "1.0.2", "scalers", "conus1.scalers")
        model_repository.save_model_file("hydrogen_emulator", "1.0.2", "surface_config", "surface_config.json")
        model_repository.save_model_file("hydrogen_emulator", "1.0.2", "subsurface_config", "subsurface_config.json")
        model_repository.set_model_implementation_class("hydrogen_emulator", "1.0.2", "lossunis.EvaluatorImplementation")

    Note when executing a model it is the responsibility of the caller to configure the python virtual environment
    with the code containing the version of the model implementation class. The class name in the environment may be anything,
    but must be associated with the version using the set_model_implementation_class method.
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
            output_file_path:   The file path to save the model file retrieved from the model repository.
            if_not_exists:      If True then do not get the file again if it already exists in output_file_path.

        This should be called from within an implementation class of a model evaluator to get the
        model files stored at training time that are required by the implemenation of that version.

        Example:

        .. code-block:: python
            model_repository.get_model_file("hydrogen_emulator", "1.0.2", "model", "model.pt")
            model_repository.get_model_file("hydrogen_emulator", "1.0.2", "subsurface_config", "subsurface.config.json")
        """

    def save_model_file(
        self, model_name: str, version: str, file_key: str, model_file_path: str
    ):
        """
        Save a model file into the model repository.

        Parameters:
            model_name:         The name of a model to be saved.
            version:            The model version to be saved.
            file_key:           Identifies the type of model file saved in the repository.
            model_file_path:    Path name to a model file to be saved into the repository.

        This should be called at training time for a model version to save model files required
        to execute the model. Saving model files in the repository means the client that executes
        the model does not need to be aware of what training files are required by the evaluator.

        Example:

        .. code-block:: python
            model_repository.save_model_file("hydrogen_emulator", "1.0.2", "model", "multistm_sept8.pt")
            model_repository.save_model_file("hydrogen_emulator", "1.0.2", "subsurface_config", "subsurface.config.json")
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
            model_repository.save_attributes("hydrogen_emulator", "1.0.2", {accuracy": "0.65", "loss": "0.5"})
            model_repository.save_attributes("hydrogen_emulator", "1.0.2", {"released": "true"})
            model_repository.save_attributes("hydrogen_emulator", "1.0.2", {"repo": "git+ssh://git@github.com/HydroFrame-ML/hydrogen-emulator-los-sunis.git@0.0.0"})
        """

    def get_attributes(self, model_name: str, version: str) -> dict:
        """
        Get all the attributes of a model version in the repository.

        Parameters:
            model_name:         The name of a model,
            version:            The model version.
        Returns:
            A dict with all the attributes associated with the model version.

        There are autogenerated attributes such as: created_at, modified_at.

        Example to get attributes of a model version.

        .. code-block:: python

            attributes = model_repository.get_attributes("hydrogen_emulator", "1.0.2")
            assert attributes.get("released") == "true"
            assert attributes.get("loss") == "0.5"
            assert attributes.get("created_at") == "2024-01-31 14:22:00"

        """

    def get_model_names() -> List[str]:
        """
        Get the list of model names in the repository.
        Returns:
            A list of model names.
        Example:

        .. code-block::python

            model_names = model_repository.get_model_names()
            assert model_names == ["hydrogen_emulator"]
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

            versions = model_repository.get_versions("hydrogen_emulator")
            assert versions = ["1.0.1", "1.0.2"]
        """
        return []
    
    def delete_version(self, model_name:str, version:str):
        """
        Delete a model version from the repository to save disk space.

        Parameters:
            model_name:         The name of a model,
            version:            The model version.

        Example:
        .. code-block:: python

            model_repository.delete_version("hydrogen_emulator", "1.0.1")
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

            executor = model_repository.get_model_implementation_class("hydrogen_emulator", "1.0.2")
            parameters = {}
            executor.execute(parameters)

        """

    def set_model_implementation_class(
        self, model_name: str, version: str, class_package_name: str
    ):
        """
        Set the class package name of the the implementation class of the model version.

        Parameters:
            model_name:         The name of a model,
            version:            The model version.
            class_package_name: The fully qualified package name of the implementation class.
            
        The class_package_name must be a class that implements the ModelExecutionInterface.

        Example to register the implementation class of a model version.

        .. code-block:: python

            model_repository.set_model_implementation_class("hydrogen_emulator", "1.0.2", "losunis.EvaluatorImplementation")
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

def copy_model(source_repository:ModelRepository, target_repository: ModelRepository, model_name:str, version:str):
    """
    Copy a model version from a source repository to a target_repository.
    
    Parameters:
        source_repository:  A source implementation of a ModelRepository.
        target_repository:  A target implementation of a ModelRepository.
        model_name:         The name of a model. If "*" copy all models.
        version:            The model version. If "*" copy all versions.
    
    This allows you copy copy a model version between repositories such as
    between an MLFlow based repository used for local development and a shared directory
    based repository used by production code to execute the model.

    Example:

    .. code-block:: python
        import hf_model_repository as mr

        target = mr.get_model_repository()
        source = mr.get_model_repository({"mlflow_url": "https://localhost/mlflow"})
        mr.copy_repository(source, target, "hydrogen_emulator", "1.0.2")
    """