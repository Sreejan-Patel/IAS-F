import React from 'react';

class MNISTDigitRecognition extends React.Component {
    handleFileUpload = (event) => {
        event.preventDefault(); // Prevent the default form submission behavior
        const formData = new FormData(); // Create FormData object

        // Append the file to the FormData object
        formData.append('image', event.target.files[0]);

        // Send the form data to the server using fetch
        fetch('http://127.0.0.1:7000/receive_input', {
            method: 'POST',
            body: formData, // Pass the FormData object as the body
        })
        .then(response => {
            if (response.ok) {
                console.log('File uploaded successfully');
            } else {
                console.error('Failed to upload file');
            }
        })
        .catch(error => {
            console.error('Error:', error);
        });
    }

    render() {
        return (
            <div>
                <h2>MNIST Digit Recognition</h2>
                <form encType="multipart/form-data">
                    <input type="file" name="image" onChange={this.handleFileUpload} />
                    <input type="submit" value="Upload" />
                </form>
            </div>
        );
    }
}

export default MNISTDigitRecognition;
