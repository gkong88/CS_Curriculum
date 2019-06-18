import nn
import numpy
import random
import pdb
import math

class PerceptronModel(object):
    def __init__(self, dimensions):
        """
        Initialize a new Perceptron instance.

        A perceptron classifies data points as either belonging to a particular
        class (+1) or not (-1). `dimensions` is the dimensionality of the data.
        For example, dimensions=2 would mean that the perceptron must classify
        2D points.
        """
        self.w = nn.Parameter(1, dimensions)

    def get_weights(self):
        """
        Return a Parameter instance with the current weights of the perceptron.
        """
        return self.w

    def run(self, x):
        """
        Calculates the score assigned by the perceptron to a data point x.

        Inputs:
            x: a node with shape (1 x dimensions)
        Returns: a node containing a single number (the score)
        """
        "*** YOUR CODE HERE ***"
        return nn.DotProduct(self.w, x)

    def get_prediction(self, x):
        """
        Calculates the predicted class for a single data point `x`.

        Returns: 1 or -1
        """
        "*** YOUR CODE HERE ***"
        if nn.as_scalar(self.run(x)) >= 0:
            return 1
        else:
            return -1

    def train(self, dataset):
        """
        Train the perceptron until convergence.
        """
        "*** YOUR CODE HERE ***"
        update = True
        dataset_size = dataset.x.shape[0]
        batch_num = 1
        D = dataset.iterate_forever(dataset_size)
        while update == True:
            update = False
            batch_num += 1
            X, Y = D.__next__()
            for x_array, y_scalar in zip(X.data, Y.data):
                x = nn.Constant(numpy.array([x_array]))
                y = nn.Constant(y_scalar)
                if self.get_prediction(x) != y_scalar:
                    update = True
                    self.w.update(x, nn.as_scalar(y))

                
            


class RegressionModel(object):
    """
    A neural network model for approximating a function that maps from real
    numbers to real numbers. The network should be sufficiently large to be able
    to approximate sin(x) on the interval [-2pi, 2pi] to reasonable precision.
    """
    def __init__(self):
        # Initialize your model parameters here
        "*** YOUR CODE HERE ***"
        # Initialize Hyperparameters
        # Hidden layer sizes: between 10 and 400
        # Batch size: between 1 and the size of the dataset. For Q2 and Q3, we require that total size of the dataset be evenly divisible by the batch size.
        # Learning rate: between 0.001 and 1.0
        # Number of hidden layers: between 1 and 3
        self.layer_size = 200
        self.batch_size = float("Inf")
        self.learning_rate = 0.3
        self.number_hidden_layers = 3
        self.is_model_initialized = False
    
    def init_model_weights(self, num_features):
        self.W_i = [nn.Parameter(num_features, self.layer_size)]
        self.b_i = [nn.Parameter(num_features, self.layer_size)]
        for i in range(self.number_hidden_layers - 1):
            self.W_i.append(nn.Parameter(self.layer_size, self.layer_size))
            self.b_i.append(nn.Parameter(num_features, self.layer_size))
        self.W_i.append(nn.Parameter(self.layer_size, 1))
        self.b_i.append(nn.Parameter(1, 1))

    def run(self, x):
        """
        Runs the model for a batch of examples.

        Inputs:
            x: a node with shape (batch_size x 1)
        Returns:
            A node with shape (batch_size x 1) containing predicted y-values
        """
        "*** YOUR CODE HERE ***"
        if not(self.is_model_initialized):
            print("model init")
            self.init_model_weights(len(x.data[0]))
            self.is_model_initialized = True
        layer_input = x
        for (i, W, b) in zip(range(len(self.W_i)), self.W_i, self.b_i):
            layer_input = nn.Linear(layer_input, W)
            layer_input = nn.AddBias(layer_input, b)
            if i < len(self.W_i) - 1:
                layer_input = nn.ReLU(layer_input)
        return layer_input

    def get_loss(self, x, y):
        """
        Computes the loss for a batch of examples.

        Inputs:
            x: a node with shape (batch_size x 1)
            y: a node with shape (batch_size x 1), containing the true y-values
                to be used for training
        Returns: a loss node
        """
        "*** YOUR CODE HERE ***"
        return nn.SquareLoss(self.run(x), y)

    def train(self, dataset):
        """
        Trains the model.
        """
        "*** YOUR CODE HERE ***"

        self.batch_size = min(self.batch_size, len(dataset.y.data))
        D = dataset.iterate_forever(self.batch_size)
        x, y = D.__next__()
        if not(self.is_model_initialized):
            self.init_model_weights(len(x.data[0]))
            self.is_model_initialized = True
        model_parameters = self.W_i
        loss = self.get_loss(x, y)
        while nn.as_scalar(loss) > 0.02:
            # gradients = nn.gradients(loss, self.W_i + self.b_i)
            gradients = nn.gradients(loss, self.W_i)
            for i, parameter, gradient in zip(range(len(model_parameters)), model_parameters, gradients):
                parameter.update(gradient, -self.learning_rate)
            loss = self.get_loss(x, y)
        
        

class DigitClassificationModel(object):
    """
    A model for handwritten digit classification using the MNIST dataset.

    Each handwritten digit is a 28x28 pixel grayscale image, which is flattened
    into a 784-dimensional vector for the purposes of this model. Each entry in
    the vector is a floating point number between 0 and 1.

    The goal is to sort each digit into one of 10 classes (number 0 through 9).

    (See RegressionModel for more information about the APIs of different
    methods here. We recommend that you implement the RegressionModel before
    working on this part of the project.)
    """
    def __init__(self):
        # Initialize your model parameters here
        "*** YOUR CODE HERE ***"
        # Initialize Hyperparameters
        # Hidden layer sizes: between 10 and 400
        # Batch size: between 1 and the size of the dataset. For Q2 and Q3, we require that total size of the dataset be evenly divisible by the batch size.
        # Learning rate: between 0.001 and 1.0
        # Number of hidden layers: between 1 and 3
        self.layer_size = 100
        self.batch_size = 100
        self.learning_rate = 0.3
        self.number_hidden_layers = 2
        self.is_model_initialized = False
    
    def init_model_weights(self, num_features):
        self.W_i = [nn.Parameter(num_features, self.layer_size)]
        self.b_i = [nn.Parameter(1, self.layer_size)]

        for i in range(self.number_hidden_layers - 1):
            self.W_i.append(nn.Parameter(self.layer_size, self.layer_size))
            self.b_i.append(nn.Parameter(1, self.layer_size))
        self.W_i.append(nn.Parameter(self.layer_size, 10))
        self.b_i.append(nn.Parameter(1, 10))

    def run(self, x):
        """
        Runs the model for a batch of examples.

        Your model should predict a node with shape (batch_size x 10),
        containing scores. Higher scores correspond to greater probability of
        the image belonging to a particular class.

        Inputs:
            x: a node with shape (batch_size x 784)
        Output:
            A node with shape (batch_size x 10) containing predicted scores
                (also called logits)
        """
        "*** YOUR CODE HERE ***"
        if not(self.is_model_initialized):
            print("model init")
            print("num features: %s"%len(x.data[0]))
            self.init_model_weights(len(x.data[0]))
            self.is_model_initialized = True
        layer_input = x
        for (i, W, b) in zip(range(len(self.W_i)), self.W_i, self.b_i):
            layer_input = nn.Linear(layer_input, W)
            layer_input = nn.AddBias(layer_input, b)
            if i < len(self.W_i) - 1:
                layer_input = nn.ReLU(layer_input)
        return layer_input        

    def get_loss(self, x, y):
        """
        Computes the loss for a batch of examples.

        The correct labels `y` are represented as a node with shape
        (batch_size x 10). Each row is a one-hot vector encoding the correct
        digit class (0-9).

        Inputs:
            x: a node with shape (batch_size x 784)
            y: a node with shape (batch_size x 10)
        Returns: a loss node
        """
        "*** YOUR CODE HERE ***"
        return nn.SoftmaxLoss(self.run(x), y)

    def train(self, dataset):
        """
        Trains the model.
        """
        "*** YOUR CODE HERE ***"
        self.batch_size = min(self.batch_size, len(dataset.y.data))
        D = dataset.iterate_forever(self.batch_size)
        x, y = D.__next__()
        if not(self.is_model_initialized):
            self.init_model_weights(len(x.data[0]))
            print("num features: %s"%len(x.data[0]))
            self.is_model_initialized = True
        model_parameters = self.W_i
        loss = self.get_loss(x, y)
        while dataset.get_validation_accuracy() < 0.98:
            # gradients = nn.gradients(loss, self.W_i + self.b_i)
            gradients = nn.gradients(loss, self.W_i)
            for i, parameter, gradient in zip(range(len(model_parameters)), model_parameters, gradients):
                parameter.update(gradient, -self.learning_rate)
            loss = self.get_loss(x, y)   
            x , y = D.__next__()

class LanguageIDModel(object):
    """
    A model for language identification at a single-word granularity.

    (See RegressionModel for more information about the APIs of different
    methods here. We recommend that you implement the RegressionModel before
    working on this part of the project.)
    """
    def __init__(self):
        # Our dataset contains words from five different languages, and the
        # combined alphabets of the five languages contain a total of 47 unique
        # characters.
        # You can refer to self.num_chars or len(self.languages) in your code
        self.num_chars = 47
        self.languages = ["English", "Spanish", "Finnish", "Dutch", "Polish"]

        # Initialize your model parameters here
        self.hidden_layer_size = 100
        self.layer_size = 50
        self.batch_size = 1000
        self.learning_rate = 0.3
        self.number_hidden_layers = 2
        self.is_model_initialized = False
    
    def init_model_weights(self):
        
        self.W_x = nn.Parameter(self.num_chars, self.hidden_layer_size)
        self.b_x = nn.Parameter(1, self.hidden_layer_size)
        self.W_h = nn.Parameter(self.hidden_layer_size, self.hidden_layer_size)
        self.b_h = nn.Parameter(1, self.hidden_layer_size)
        self.W_y_layers = [nn.Parameter(self.hidden_layer_size, self.layer_size)]
        self.b_y_layers = [nn.Parameter(1, self.layer_size)]
        for i in range(self.number_hidden_layers - 1):
            self.W_y_layers.append(nn.Parameter(self.layer_size, self.layer_size))
            self.b_y_layers.append(nn.Parameter(1, self.layer_size))
        self.W_y_layers.append(nn.Parameter(self.layer_size, 5))
        self.b_y_layers.append(nn.Parameter(1, 5))

    def run(self, xs):
        """
        Runs the model for a batch of examples.

        Although words have different lengths, our data processing guarantees
        that within a single batch, all words will be of the same length (L).

        Here `xs` will be a list of length L. Each element of `xs` will be a
        node with shape (batch_size x self.num_chars), where every row in the
        array is a one-hot vector encoding of a character. For example, if we
        have a batch of 8 three-letter words where the last word is "cat", then
        xs[1] will be a node that contains a 1 at position (7, 0). Here the
        index 7 reflects the fact that "cat" is the last word in the batch, and
        the index 0 reflects the fact that the letter "a" is the inital (0th)
        letter of our combined alphabet for this task.

        Your model should use a Recurrent Neural Network to summarize the list
        `xs` into a single node of shape (batch_size x hidden_size), for your
        choice of hidden_size. It should then calculate a node of shape
        (batch_size x 5) containing scores, where higher scores correspond to
        greater probability of the word originating from a particular language.

        Inputs:
            xs: a list with L elements (one per character), where each element
                is a node with shape (batch_size x self.num_chars)
        Returns:
            A node with shape (batch_size x 5) containing predicted scores
                (also called logits)
        """
        "*** YOUR CODE HERE ***"
        if not(self.is_model_initialized):
            self.init_model_weights()
            self.is_model_initialized = True
        h = nn.Linear(xs[0], self.W_x)
        h = nn.AddBias(h, self.b_x)
        for x in xs[1:]:
            h = nn.ReLU(nn.Add(nn.AddBias(nn.Linear(x, self.W_x), self.b_x), 
                               nn.AddBias(nn.Linear(h, self.W_h), self.b_h)))
        # x is the vector summary of the input
        x = h
        for i in range(len(self.W_y_layers)):
            x = nn.Linear(x, self.W_y_layers[i])
            x = nn.AddBias(x, self.b_y_layers[i])
            x = nn.ReLU(x)
        return x
        

    def get_loss(self, xs, y):
        """
        Computes the loss for a batch of examples.

        The correct labels `y` are represented as a node with shape
        (batch_size x 5). Each row is a one-hot vector encoding the correct
        language.

        Inputs:
            xs: a list with L elements (one per character), where each element
                is a node with shape (batch_size x self.num_chars)
            y: a node with shape (batch_size x 5)
        Returns: a loss node
        """
        "*** YOUR CODE HERE ***"
        return nn.SoftmaxLoss(self.run(xs), y)

    def train(self, dataset):
        """
        Trains the model.
        """
        "*** YOUR CODE HERE ***"
        batch_size = min(self.batch_size, len(dataset.train_y.data))
        D_train_size = len(dataset.train_y.data)
        if not(self.is_model_initialized):
            self.init_model_weights()
            self.is_model_initialized = True
        model_parameters = self.W_y_layers + self.b_y_layers + [self.b_x] + [self.b_h] + [self.W_h] + [self.W_x]
        E_train = 1
        num_epochs = 0
        while E_train > 0.11:
            num_epochs +=1
            epoch_errors = 0
            D = dataset.iterate_once(batch_size)
            loss_s = []
            for xs, y in D:
                loss = self.get_loss(xs, y)
                loss_s.append(nn.as_scalar(loss))
                epoch_errors += self.batch_errors(self.run(xs), y)
                gradients = nn.gradients(loss, model_parameters)
                for parameter, gradient in zip(model_parameters, gradients):
                    parameter.update(gradient, -self.learning_rate)
            E_train = epoch_errors / D_train_size
            self.learning_rate = 0.001 + (E_train/2 + (E_train / 2) ** 2) * 0.999
            
            print("epoch, average softmax error, percent error: %s, %s, %s"%(num_epochs, sum(loss_s)/len(loss_s), E_train))
            if num_epochs > 400 or (num_epochs > 10 and E_train > 0.7):
                num_epochs = 0
                self.init_model_weights()
                model_parameters = self.W_y_layers + self.b_y_layers + [self.b_x] + [self.b_h] + [self.W_h] + [self.W_x]
    def batch_errors(self, y1, y2):
        errors = 0
        for i in range(len(y1.data)):
            if y1.data[i].argmax() != y2.data[i].argmax():
                errors += 1
        return errors

