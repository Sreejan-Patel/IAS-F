import React from "react";
import Display from "./display"; // Assuming Display component is in display.js file

class ParentComponent extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            predictedDigit: null
        };
    }

    componentDidMount() {
        // Assuming you're making a fetch request to the server to get the predicted digit
        fetch('/display', {
            method: 'POST',
            // Add any necessary headers or body for the request
        })
        .then(response => response.json())
        .then(data => {
            // Update state with the predicted digit received from the server
            this.setState({ predictedDigit: data.predicted_digit });
        })
        .catch(error => {
            console.error('Error fetching data:', error);
        });
    }

    render() {
        return (
            <div>
                <h1>Parent Component</h1>
                <Display digit={this.state.predictedDigit} />
            </div>
        );
    }
}

export default ParentComponent;
